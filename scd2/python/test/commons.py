import pandas as pd
import sys
import os
import logging
import trino
import numpy as np
np.set_printoptions(threshold=np.inf)

from datetime import date, timedelta, datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_table, render_data, diff_with_color
from scd2_trino import TrinoSCD2Strategy
from scd2_spark import SparkSCD2Strategy
from constants import MAX_TS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRINO_USER = get_credential('TRINO_USER', 'trino')
TRINO_PASSWORD = get_credential('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'iceberg_hive')
TRINO_SCHEMA = get_param('TRINO_SCHEMA', 'default')
TRINO_USE_SSL = get_param('TRINO_USE_SSL', 'true').lower() in ('true', '1', 't')

# Connect to MinIO or AWS S3
S3_ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')

S3_WAREHOUSE_BUCKET = get_param('S3_WAREHOUSE_BUCKET', 'warehouse-bucket')
S3_WAREHOUSE_BUCKET = replace_vars_in_string(S3_WAREHOUSE_BUCKET, { "zone": "", "env": "" } )
S3_WAREHOUSE_PREFIX = get_param('S3_WAREHOUSE_PREFIX', 'iceberg-poc')
S3_WAREHOUSE_PREFIX = replace_vars_in_string(S3_WAREHOUSE_PREFIX, { "zone": "", "env": "" } )
S3_UPLOAD_BUCKET = get_param('S3_UPLOAD_BUCKET', 'upload-bucket')
S3_UPLOAD_BUCKET = replace_vars_in_string(S3_UPLOAD_BUCKET, { "zone": "", "env": "" } )
S3_UPLOAD_PREFIX = get_param('S3_UPLOAD_PREFIX', 'iceberg-poc')
S3_UPLOAD_PREFIX = replace_vars_in_string(S3_UPLOAD_PREFIX, { "zone": "", "env": "" } )
AWS_ACCESS_KEY = get_credential('AWS_ACCESS_KEY', None)
AWS_SECRET_ACCESS_KEY = get_credential('AWS_SECRET_ACCESS_KEY', None)
DOWNLOAD_TEST_CASES_FROM_S3 = get_param('DOWNLOAD_TEST_CASES_FROM_S3', 'false').lower() in ('true', '1', 't')

RAW_TABLE_NAME="raw_person"
SCD2_TABLE_NAME="dim_person"
SCD2_VIEW_NAME="view_person_scd2"

EXCLUDE_COLS = ["record_hash","dp_load_timestamp", "change_type"]
LOAD_TS_COL="dp_loaded_at"


# ---------------------------------------------------------------------------
# Base class — all shared test-helper logic lives here
# ---------------------------------------------------------------------------

class TestCommonsBase:
    """Base class for test commons. Subclasses supply COLS_WITH_TYPE and _make_strategy."""

    COLS_WITH_TYPE: list  # defined in each concrete subclass

    def _make_strategy(self, ctx):
        raise NotImplementedError

    def _execute_insert(self, ins_stmt: str, ctx):
        raise NotImplementedError

    def _execute_select(self, sel_stmt: str, ctx):
        return self._make_strategy(ctx).get_table_data(sel_stmt)

    def create_raw_table(self, ctx, pk_columns_with_type: list = ["id INT"]):
        raise NotImplementedError

    def raw_table_fqn(self, ctx):
        return self._make_strategy(ctx).raw_table_fqn()
    
    def scd2_table_fqn(self, ctx):
        return self._make_strategy(ctx).scd2_table_fqn()
    
    def scd2_intermediary_table_fqn(self, ctx):
        return self._make_strategy(ctx).scd2_intermediary_table_fqn()

    def get_table_data(self, ctx, table_name: str, exclude_cols: list = [], order_by_cols: list = [], for_version: str = None):
        """Read a table via the active strategy and return a pandas DataFrame."""
        if isinstance(exclude_cols, str):
            exclude_cols = [exclude_cols]
        return self._make_strategy(ctx).get_table_data(table_name, exclude_cols=exclude_cols, order_by_cols=order_by_cols, for_version=for_version)

    def create_dim_table_for_test(self, ctx, pk_columns_with_type: list = ["id INT"]):
        self._make_strategy(ctx).create_dim_table(
            self.scd2_table_fqn(ctx),
            s3_warehouse_bucket=S3_WAREHOUSE_BUCKET,
            s3_warehouse_prefix=S3_WAREHOUSE_PREFIX,
            pk_columns_with_type=pk_columns_with_type,
            cols_with_type=self.COLS_WITH_TYPE,
            partition_cols=["dp_ts_from"],
            sort_cols=[],
        )

    def normalize_ts_for_assert(value):
        if pd.isna(value):
            return None
        if hasattr(value, "isoformat"):
            value = value.isoformat(sep=" ")
        else:
            value = str(value)
        if value.endswith("+00:00"):
            value = value[:-6]
        return value    
    
    def assert_df(self, actual_df: pd.DataFrame, expected_df: pd.DataFrame, output_file_name: str):
        for col in ["dp_ts_to", "dp_ts_from", "dp_created_at", "dp_replaced_at"]:
            if col in actual_df.columns and col in expected_df.columns:
                actual_df[col] = actual_df[col].apply(TestCommonsBase.normalize_ts_for_assert)
                expected_df[col] = expected_df[col].apply(TestCommonsBase.normalize_ts_for_assert)
        try:
            pd.testing.assert_frame_equal(actual_df, expected_df, check_dtype=False, check_like=False)
        except AssertionError as e:
            error_msg = f"### Assertion Error\n\n```\n{str(e)}\n```\n"
            render_data(error_msg, output_file_name=output_file_name)
            raise

    def scd2_merge_as_preparation(self, ctx, ins_stmts: list, load_ts_list: list, current_ts_list: list, perform_merge_op: bool = True, use_delta_mode_for_raw_table: bool = False, display_result: bool = True, expected=None, output_file_name: str = None, test_description: str = None, pk_columns: list = ["id"]):
        render_data(test_description, output_file_name=output_file_name)

        for idx, ins_stmt in enumerate(ins_stmts):
            self._execute_insert(ins_stmt, ctx)

            print(load_ts_list[idx], current_ts_list[idx])

            self._make_strategy(ctx).merge_into_dim_table(
                raw_table_name=RAW_TABLE_NAME,
                dim_table_name=SCD2_TABLE_NAME,
                scd2_view_name=SCD2_VIEW_NAME,
                load_ts=load_ts_list[idx],
                load_ts_col="dp_loaded_at",
                pk_columns=pk_columns,
                cols_with_type=self.COLS_WITH_TYPE,
                current_ts=current_ts_list[idx],
                use_delta_mode_for_raw_table=use_delta_mode_for_raw_table,
                perform_merge_op=perform_merge_op,
                show_input_to_merge=True,
            )

        render_data("### Perform Preparation", output_file_name=output_file_name)

        df_raw = self.get_table_data(ctx, RAW_TABLE_NAME, order_by_cols=["dp_loaded_at"]+pk_columns)
        render_table(df_raw, title=f"Raw Table `{RAW_TABLE_NAME}`", output_file_name=output_file_name)

        if display_result:
            df = self.get_table_data(ctx, SCD2_TABLE_NAME, order_by_cols=pk_columns+["dp_ts_from"])
            render_table(df, title=f"Dimensional Table `{SCD2_TABLE_NAME}`", exclude_cols=EXCLUDE_COLS, output_file_name=output_file_name)

        if expected is not None:
            actual_df = self.get_table_data(ctx, SCD2_TABLE_NAME, order_by_cols=pk_columns+["dp_ts_from"], exclude_cols=["dp_key"])
            expected_df = pd.DataFrame(expected, columns=actual_df.columns)
            
            self.assert_df(actual_df, expected_df, output_file_name=output_file_name)

    def scd2_merge_as_test(self, ctx, test_step: int, ins_stmt: str, load_ts: datetime, current_ts: datetime, expected=None, output_file_name: str = None, test_description: str = None, test_after_description: str = None, perform_merge_op: bool = True, use_delta_mode_for_raw_table: bool = False, display_result: bool = True, show_input_to_merge: bool = True, pk_columns: list = ["id"]):
        self._execute_insert(ins_stmt, ctx)

        render_data(f"## Test Step {test_step}", output_file_name=output_file_name)
        render_data(test_description, output_file_name=output_file_name)

        df_dim_before = self.get_table_data(ctx, SCD2_TABLE_NAME, order_by_cols=pk_columns+["dp_ts_from"])
        df_raw = self.get_table_data(ctx, RAW_TABLE_NAME, order_by_cols=["dp_loaded_at"]+pk_columns)
        render_table(df_raw, title=f"Raw Table `{RAW_TABLE_NAME}`", output_file_name=output_file_name)

        self._make_strategy(ctx).merge_into_dim_table(
            raw_table_name=RAW_TABLE_NAME,
            dim_table_name=SCD2_TABLE_NAME,
            scd2_view_name=SCD2_VIEW_NAME,
            load_ts=load_ts,
            load_ts_col="dp_loaded_at",
            pk_columns=pk_columns,
            cols_with_type=self.COLS_WITH_TYPE,
            current_ts=current_ts,
            use_delta_mode_for_raw_table=use_delta_mode_for_raw_table,
            perform_merge_op=perform_merge_op,
            show_input_to_merge=show_input_to_merge,
            output_file_name=output_file_name,
        )

        if display_result:
            df = self.get_table_data(ctx, SCD2_TABLE_NAME, order_by_cols=pk_columns+["dp_ts_from"])
            df_colored = diff_with_color(df_dim_before, df, index_cols=["dp_key"], sort_cols=pk_columns+["dp_ts_from"])
            render_table(df_colored, title=f"Dimensional Table `{SCD2_TABLE_NAME}`", decscription=test_after_description, exclude_cols=EXCLUDE_COLS, output_file_name=output_file_name)
            render_data(test_after_description, output_file_name=output_file_name)

        actual_df = self.get_table_data(ctx, SCD2_TABLE_NAME, order_by_cols=pk_columns+["dp_ts_from"], exclude_cols=["dp_key"])
        expected_df = pd.DataFrame(expected, columns=actual_df.columns)

        self.assert_df(actual_df, expected_df, output_file_name=output_file_name)

    def scd2_merge_as_test2(self, ctx, test_step, load_ts_list: list, current_ts_list: list, expected=None, output_file_name: str = None, test_description: str = None, test_after_description: str = None, perform_merge_op: bool = True, use_delta_mode_for_raw_table: bool = False, display_result: bool = True, show_input_to_merge: bool = True, pk_columns: list = ["id"]):
        render_data(f"## Test Step {test_step}", output_file_name=output_file_name)
        render_data(test_description, output_file_name=output_file_name)

        df_dim_before = self.get_table_data(ctx, SCD2_TABLE_NAME, order_by_cols=pk_columns+["dp_ts_from"])
        df_raw = self.get_table_data(ctx, RAW_TABLE_NAME, order_by_cols=["dp_loaded_at"]+pk_columns)
        render_table(df_raw, title=f"Raw Table `{RAW_TABLE_NAME}`", output_file_name=output_file_name)

        strategy = self._make_strategy(ctx)
        for idx, load_ts in enumerate(load_ts_list):
            strategy.merge_into_dim_table(
                raw_table_name=RAW_TABLE_NAME,
                dim_table_name=SCD2_TABLE_NAME,
                scd2_view_name=SCD2_VIEW_NAME,
                load_ts=load_ts_list[idx],
                load_ts_col="dp_loaded_at",
                pk_columns=pk_columns,
                cols_with_type=self.COLS_WITH_TYPE,
                current_ts=current_ts_list[idx],
                use_delta_mode_for_raw_table=use_delta_mode_for_raw_table,
                perform_merge_op=perform_merge_op,
                show_input_to_merge=show_input_to_merge,
                output_file_name=output_file_name,
            )

        if display_result:
            df = self.get_table_data(ctx, SCD2_TABLE_NAME, order_by_cols=pk_columns+["dp_ts_from"])
            df_colored = diff_with_color(df_dim_before, df, index_cols=["dp_key"], sort_cols=pk_columns+["dp_ts_from"])
            render_table(df_colored, title=f"Dimensional Table `{SCD2_TABLE_NAME}`", decscription=test_after_description, exclude_cols=EXCLUDE_COLS, output_file_name=output_file_name)
            render_data(test_after_description, output_file_name=output_file_name)

        actual_df = self.get_table_data(ctx, SCD2_TABLE_NAME, order_by_cols=pk_columns+["dp_ts_from"], exclude_cols=["dp_key"])
        expected_df = pd.DataFrame(expected, columns=actual_df.columns)

        self.assert_df(actual_df, expected_df, output_file_name=output_file_name)

    def scd2_sel_as_test(self, ctx, sel_stmt: str, expected=None, output_file_name: str = None, test_description: str = None, test_after_description: str = None, perform_merge_op: bool = True, display_result: bool = True, show_input_to_merge: bool = True):
        actual_df = self._execute_select(sel_stmt, ctx)

        if display_result:
            render_data("### Perform Test", output_file_name=output_file_name)
            render_data(test_description, output_file_name=output_file_name)
            render_data(f"\n\n`{sel_stmt}`\n", output_file_name=output_file_name)
            render_table(actual_df, title=f"Dimensional Table `{SCD2_TABLE_NAME}`", output_file_name=output_file_name)
            render_data(test_after_description, output_file_name=output_file_name)

        expected_df = pd.DataFrame(expected, columns=actual_df.columns)
        self.assert_df(actual_df, expected_df, output_file_name=output_file_name)

    def optimize_table(self, ctx, table_name: str) -> None:
        self._make_strategy(ctx).optimize_table(table_name)

    def analyze_table(self, ctx, table_name: str) -> None:
        self._make_strategy(ctx).analyze_table(table_name)


# ---------------------------------------------------------------------------
# Concrete strategy-specific subclasses
# ---------------------------------------------------------------------------

class TrinoTestCommons(TestCommonsBase):
    COLS_WITH_TYPE_TRINO = [
            "first_name VARCHAR",
            "last_name VARCHAR",
            "city VARCHAR",
            "email VARCHAR",
        ]
    COLS_WITH_TYPE = COLS_WITH_TYPE_TRINO

    def _make_strategy(self, ctx):
        return TrinoSCD2Strategy(ctx.conn, s3_client=ctx.s3_client, catalog=TRINO_CATALOG, schema=TRINO_SCHEMA, raw_table_name=RAW_TABLE_NAME, scd2_table_name=SCD2_TABLE_NAME)

    def create_raw_table(self, ctx, pk_columns_with_type: list = ["id INT"]):
        cursor = ctx.conn.cursor()

        drop_table_sql = f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}"
        cursor.execute(drop_table_sql)
        logger.debug(f"Table {RAW_TABLE_NAME} dropped successfully (if it existed).")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME} (
            {", ".join(pk_columns_with_type)},
            first_name VARCHAR,
            last_name VARCHAR,
            city VARCHAR,
            email VARCHAR,
            status VARCHAR,
            dp_ts_from TIMESTAMP,
            dp_loaded_at TIMESTAMP
        )
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['dp_loaded_at']
        )
        """

        cursor.execute(create_table_sql)
        logger.debug(f"Table {RAW_TABLE_NAME} created successfully (or already exists).")

    def _execute_insert(self, ins_stmt: str, ctx):
        cursor = ctx.conn.cursor()
        cursor.execute(ins_stmt)

    def _execute_select(self, sel_stmt: str, ctx) -> pd.DataFrame:
        df = pd.read_sql_query(sel_stmt, ctx.conn)
        return df

class SparkTestCommons(TestCommonsBase):
    COLS_WITH_TYPE_SPARK = [
            "first_name STRING",
            "last_name STRING",
            "city STRING",
            "email STRING",
        ]
    COLS_WITH_TYPE = COLS_WITH_TYPE_SPARK

    def _make_strategy(self, ctx):
        return SparkSCD2Strategy(ctx.spark, s3_client=ctx.s3_client, database="default", raw_table_name=RAW_TABLE_NAME, scd2_table_name=SCD2_TABLE_NAME)

    def create_raw_table(self, ctx, pk_columns_with_type: list = ["id INT"]):
        drop_table_sql = f"DROP TABLE IF EXISTS default.{RAW_TABLE_NAME}"
        ctx.spark.sql(drop_table_sql)
        logger.debug(f"Table {RAW_TABLE_NAME} dropped successfully (if it existed).")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS default.{RAW_TABLE_NAME} (
            {", ".join(pk_columns_with_type)},
            first_name STRING,
            last_name STRING,
            city STRING,
            email STRING,
            status STRING,
            dp_ts_from TIMESTAMP,
            dp_loaded_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (dp_loaded_at)
        LOCATION 's3a://{S3_WAREHOUSE_BUCKET}/{S3_WAREHOUSE_PREFIX}/default/{RAW_TABLE_NAME}'
        """

        ctx.spark.sql(create_table_sql)
        logger.debug(f"Table {RAW_TABLE_NAME} created successfully (or already exists).")

    def _execute_insert(self, ins_stmt, ctx):
        ctx.spark.sql(ins_stmt)

    def _execute_select(self, sel_stmt: str, ctx) -> pd.DataFrame:
        df = ctx.spark.sql(sel_stmt).toPandas()
        return df

# ---------------------------------------------------------------------------
# Active implementation — switch here to run tests against a different engine
# ---------------------------------------------------------------------------

_impl: TestCommonsBase = SparkTestCommons()

# Re-export COLS_WITH_TYPE so test files can import it directly from commons
COLS_WITH_TYPE = _impl.COLS_WITH_TYPE


# ---------------------------------------------------------------------------
# Module-level wrappers — kept for backward compatibility with test imports
# ---------------------------------------------------------------------------

def create_raw_table(ctx, pk_columns_with_type: list = ["id INT"]):
    return _impl.create_raw_table(ctx, pk_columns_with_type=pk_columns_with_type)

def create_dim_table_for_test(ctx, pk_columns_with_type: list = ["id INT"]):
    return _impl.create_dim_table_for_test(ctx, pk_columns_with_type=pk_columns_with_type)

def scd2_merge_as_preparation(ctx, ins_stmts: list, load_ts_list: list, current_ts_list: list, perform_merge_op: bool = True, use_delta_mode_for_raw_table: bool = False, display_result: bool = True, expected=None, output_file_name: str = None, test_description: str = None, pk_columns: list = ["id"]):
    return _impl.scd2_merge_as_preparation(ctx, ins_stmts=ins_stmts, load_ts_list=load_ts_list, current_ts_list=current_ts_list, perform_merge_op=perform_merge_op, use_delta_mode_for_raw_table=use_delta_mode_for_raw_table, display_result=display_result, expected=expected, output_file_name=output_file_name, test_description=test_description, pk_columns=pk_columns)

def scd2_merge_as_test(ctx, test_step: int, ins_stmt: str, load_ts: datetime, current_ts: datetime, expected=None, output_file_name: str = None, test_description: str = None, test_after_description: str = None, perform_merge_op: bool = True, use_delta_mode_for_raw_table: bool = False, display_result: bool = True, show_input_to_merge: bool = True, pk_columns: list = ["id"]):
    return _impl.scd2_merge_as_test(ctx, test_step=test_step, ins_stmt=ins_stmt, load_ts=load_ts, current_ts=current_ts, expected=expected, output_file_name=output_file_name, test_description=test_description, test_after_description=test_after_description, perform_merge_op=perform_merge_op, use_delta_mode_for_raw_table=use_delta_mode_for_raw_table, display_result=display_result, show_input_to_merge=show_input_to_merge, pk_columns=pk_columns)

def scd2_merge_as_test2(ctx, test_step, load_ts_list: list, current_ts_list: list, expected=None, output_file_name: str = None, test_description: str = None, test_after_description: str = None, perform_merge_op: bool = True, use_delta_mode_for_raw_table: bool = False, display_result: bool = True, show_input_to_merge: bool = True, pk_columns: list = ["id"]):
    return _impl.scd2_merge_as_test2(ctx, test_step=test_step, load_ts_list=load_ts_list, current_ts_list=current_ts_list, expected=expected, output_file_name=output_file_name, test_description=test_description, test_after_description=test_after_description, perform_merge_op=perform_merge_op, use_delta_mode_for_raw_table=use_delta_mode_for_raw_table, display_result=display_result, show_input_to_merge=show_input_to_merge, pk_columns=pk_columns)

def scd2_sel_as_test(ctx, sel_stmt: str, expected=None, output_file_name: str = None, test_description: str = None, test_after_description: str = None, perform_merge_op: bool = True, display_result: bool = True, show_input_to_merge: bool = True):
    return _impl.scd2_sel_as_test(ctx, sel_stmt=sel_stmt, expected=expected, output_file_name=output_file_name, test_description=test_description, test_after_description=test_after_description, perform_merge_op=perform_merge_op, display_result=display_result, show_input_to_merge=show_input_to_merge)

def optimize_table(ctx, table_name: str) -> None:
    return _impl.optimize_table(ctx, table_name)

def analyze_table(ctx, table_name: str) -> None:
    return _impl.analyze_table(ctx, table_name)
