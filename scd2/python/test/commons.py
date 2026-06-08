import logging
import os
import sys
from decimal import Decimal
from typing import Optional

import numpy as np
import pandas as pd
import trino
from pyspark.sql import DataFrame

from datetime import date, datetime, timedelta
from common_utils import get_credential, get_param, replace_vars_in_string

np.set_printoptions(threshold=np.inf)

from lib.constants import MAX_TS
from lib.scd2_pyspark import PySparkSCD2Strategy
from lib.scd2_spark import SparkSCD2Strategy
from lib.scd2_strategy import SCD2Strategy, SCD2Table
from lib.scd2_trino import TrinoSCD2Strategy
from lib.util import (
    diff_with_color,
    render_data,
    render_table
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRINO_USER = get_credential("TRINO_USER", "trino")
TRINO_PASSWORD = get_credential("TRINO_PASSWORD", "")
TRINO_HOST = get_param("TRINO_HOST", "localhost")
TRINO_PORT = get_param("TRINO_PORT", "28082")
TRINO_CATALOG = get_param("TRINO_CATALOG", "iceberg_hive")
TRINO_SCHEMA = get_param("TRINO_SCHEMA", "default")
TRINO_USE_SSL = get_param("TRINO_USE_SSL", "true").lower() in ("true", "1", "t")

# Connect to MinIO or AWS S3
S3_ENDPOINT_URL = get_param("S3_ENDPOINT_URL", "http://localhost:9000")

S3_WAREHOUSE_BUCKET = get_param("S3_WAREHOUSE_BUCKET", "warehouse-bucket")
S3_WAREHOUSE_BUCKET = replace_vars_in_string(
    S3_WAREHOUSE_BUCKET, {"zone": "", "env": ""}
)
S3_WAREHOUSE_PREFIX = get_param("S3_WAREHOUSE_PREFIX", "iceberg-poc")
S3_WAREHOUSE_PREFIX = replace_vars_in_string(
    S3_WAREHOUSE_PREFIX, {"zone": "", "env": ""}
)
S3_UPLOAD_BUCKET = get_param("S3_UPLOAD_BUCKET", "upload-bucket")
S3_UPLOAD_BUCKET = replace_vars_in_string(S3_UPLOAD_BUCKET, {"zone": "", "env": ""})
S3_UPLOAD_PREFIX = get_param("S3_UPLOAD_PREFIX", "iceberg-poc")
S3_UPLOAD_PREFIX = replace_vars_in_string(S3_UPLOAD_PREFIX, {"zone": "", "env": ""})
AWS_ACCESS_KEY = get_credential("AWS_ACCESS_KEY", None)
AWS_SECRET_ACCESS_KEY = get_credential("AWS_SECRET_ACCESS_KEY", None)
DOWNLOAD_TEST_CASES_FROM_S3 = get_param(
    "DOWNLOAD_TEST_CASES_FROM_S3", "false"
).lower() in ("true", "1", "t")

RAW_TABLE_NAME = "raw_person"
SCD2_TABLE_NAME = "dim_person"
SCD2_VIEW_NAME = "view_person_scd2"

EXCLUDE_COLS = ["dp_record_hash"]
col_dp_ts_filter = "dp_loaded_at"

DELTA_MODE_DELETE_EXPRESSION="status = 'INACTIVE'"


# ---------------------------------------------------------------------------
# Base class — all shared test-helper logic lives here
# ---------------------------------------------------------------------------


class TestCommonsBase:
    """Base class for test commons. Subclasses supply COLS_WITH_TYPE and _make_strategy."""

    def _make_strategy(self, ctx, cols_bks: list = ["id"], cols_bks_with_type: list = ["id INT"], table_shape: str = "flat",
                       use_logical_delete_for_source_table: bool = False, 
                       check_physical_delete_against_source_table: bool = True,
                       perform_merge_op: bool = True,
                       col_dp_ts: str = "dp_ts_from", col_dp_ts_filter: str = "dp_loaded_at"):
        raise NotImplementedError

    def _execute_insert(self, ins_stmt: str, ctx):
        raise NotImplementedError

    def execute_select(self, _sel_stmt: str, _ctx)  -> pd.DataFrame:
        raise NotImplementedError

    def get_strategy_name(self):
        raise NotImplementedError

    def create_raw_table(self, ctx, cols_bks_with_type: list = ["id INT"], table_shape: str = "flat"):
        raise NotImplementedError
    
    def create_scd2_table_for_test(self, ctx, cols_bks_with_type: list = ["id INT"], table_shape: str = "flat") -> None:
        raise NotImplementedError

    def cols_with_type(self, table_form: str = "flat") -> list:
        raise NotImplementedError

    def source_table_fqn(self, ctx, iceberg_meta_tablename: str = None):
        return self._make_strategy(ctx).source_table_fqn(
            iceberg_meta_tablename=iceberg_meta_tablename
        )

    def scd2_table_fqn(self, ctx, iceberg_meta_tablename: str = None):
        return self._make_strategy(ctx).scd2_table_fqn(
            iceberg_meta_tablename=iceberg_meta_tablename
        )

    def scd2_intermediary_table_fqn(self, ctx, iceberg_meta_tablename: str = None):
        return self._make_strategy(ctx).scd2_intermediary_table_fqn(
            iceberg_meta_tablename=iceberg_meta_tablename
        )

    def get_table_data(
        self,
        ctx,
        table: SCD2Table,
        iceberg_meta_tablename: str = None,
        exclude_cols: list = [],
        order_by_cols: list = [],
        for_version: str = None,
    ):
        """Read a table via the active strategy and return a pandas DataFrame."""
        if isinstance(exclude_cols, str):
            exclude_cols = [exclude_cols]
        return self._make_strategy(ctx).get_table_data(
            table,
            iceberg_meta_tablename=iceberg_meta_tablename,
            exclude_cols=exclude_cols,
            order_by_cols=order_by_cols,
            for_version=for_version,
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

    def assert_df(
        self, actual_df: pd.DataFrame, expected_df: pd.DataFrame, output_file_name: str
    ):
        for col in ["dp_ts_to", "dp_ts_from", "dp_load_ts", "dp_replace_ts"]:
            if col in actual_df.columns and col in expected_df.columns:
                actual_df[col] = actual_df[col].apply(
                    TestCommonsBase.normalize_ts_for_assert
                )
                expected_df[col] = expected_df[col].apply(
                    TestCommonsBase.normalize_ts_for_assert
                )
        for df in (actual_df, expected_df):
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, Decimal)).any():
                    df[col] = df[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
        try:
            pd.testing.assert_frame_equal(
                actual_df, expected_df, check_dtype=False, check_like=False
            )
        except AssertionError as e:
            error_msg = f"### Assertion Error\n\n```\n{str(e)}\n```\n"
            render_data(error_msg, output_file_name=output_file_name)
            raise

    def insert_as_preparation(self, ctx, ins_stmts: list):
        for idx, ins_stmt in enumerate(ins_stmts):
            self._execute_insert(ins_stmt, ctx)

    def scd2_merge_as_preparation(
        self,
        ctx,
        ins_stmts: list,
        dp_ts_list: list,
        current_ts_list: list,
        perform_merge_op: bool = True,
        use_logical_delete_for_source_table: bool = False,
        logical_delete_expression: Optional[str] = None,
        check_physical_delete_against_source_table: bool = True,
        display_result: bool = True,
        expected=None,
        output_file_name: str = None,
        test_description: str = None,
        cols_bks: list = ["id"],
    ):
        render_data(test_description, output_file_name=output_file_name)

        for idx, ins_stmt in enumerate(ins_stmts):
            self._execute_insert(ins_stmt, ctx)

            self._make_strategy(ctx, cols_bks=cols_bks,
                                use_logical_delete_for_source_table=use_logical_delete_for_source_table,
                                logical_delete_expression=logical_delete_expression,
                                check_physical_delete_against_source_table=check_physical_delete_against_source_table,
                                perform_merge_op=perform_merge_op).merge_into_scd2_table(
                dp_ts=dp_ts_list[idx],
                current_ts=current_ts_list[idx],
                show_input_to_merge=True,
            )

        render_data("### Perform Preparation", output_file_name=output_file_name)

        df_raw = self.get_table_data(
            ctx, SCD2Table.RAW, order_by_cols=["dp_loaded_at"] + cols_bks
        )
        render_table(
            df_raw,
            title=f"Raw Table `{RAW_TABLE_NAME}`",
            output_file_name=output_file_name,
        )

        if display_result:
            df = self.get_table_data(
                ctx, SCD2Table.SCD2, order_by_cols=cols_bks + ["dp_ts_from"]
            )
            render_table(
                df,
                title=f"Dimensional Table `{SCD2_TABLE_NAME}`",
                exclude_cols=EXCLUDE_COLS,
                output_file_name=output_file_name,
            )

        if expected is not None:
            actual_df = self.get_table_data(
                ctx,
                table=SCD2Table.SCD2,
                order_by_cols=cols_bks + ["dp_ts_from"],
                exclude_cols=["dp_record_id"],
            )
            expected_df = pd.DataFrame(expected, columns=actual_df.columns)

            self.assert_df(actual_df, expected_df, output_file_name=output_file_name)

    def scd2_merge_as_test(
        self,
        ctx,
        test_step: int,
        ins_stmt: str,
        dp_ts: datetime,
        current_ts: datetime,
        expected=None,
        output_file_name: str = None,
        test_description: str = None,
        test_after_description: str = None,
        perform_merge_op: bool = True,
        use_logical_delete_for_source_table: bool = False,
        logical_delete_expression: Optional[str] = None,
        check_physical_delete_against_source_table: bool = True,
        display_result: bool = True,
        show_input_to_merge: bool = True,
        cols_bks: list = ["id"],
        table_shape: str = "flat",        
    ):
        self._execute_insert(ins_stmt, ctx)

        render_data(f"## Test Step {test_step}", output_file_name=output_file_name)
        render_data(test_description, output_file_name=output_file_name)

        df_dim_before = self.get_table_data(
            ctx, SCD2Table.SCD2, order_by_cols=cols_bks + ["dp_ts_from"]
        )
        df_raw = self.get_table_data(
            ctx, SCD2Table.RAW, order_by_cols=["dp_loaded_at"] + cols_bks
        )
        render_table(
            df_raw,
            title=f"Raw Table `{RAW_TABLE_NAME}`",
            output_file_name=output_file_name,
        )

        self._make_strategy(ctx, cols_bks=cols_bks, table_shape=table_shape,
                            use_logical_delete_for_source_table=use_logical_delete_for_source_table, 
                            logical_delete_expression=logical_delete_expression,
                            check_physical_delete_against_source_table=check_physical_delete_against_source_table,
                            perform_merge_op=perform_merge_op).merge_into_scd2_table(
            dp_ts=dp_ts,
            current_ts=current_ts,
            show_input_to_merge=show_input_to_merge,
            output_file_name=output_file_name,
        )

        if display_result:
            df = self.get_table_data(
                ctx, SCD2Table.SCD2, order_by_cols=cols_bks + ["dp_ts_from"]
            )
            df_colored = diff_with_color(
                df_dim_before,
                df,
                index_cols=["dp_record_id"],
                sort_cols=cols_bks + ["dp_ts_from"],
            )
            render_table(
                df_colored,
                title=f"Dimensional Table `{SCD2_TABLE_NAME}`",
                decscription=test_after_description,
                exclude_cols=EXCLUDE_COLS,
                output_file_name=output_file_name,
            )
            render_data(test_after_description, output_file_name=output_file_name)

        actual_df = self.get_table_data(
            ctx,
            SCD2Table.SCD2,
            order_by_cols=cols_bks + ["dp_ts_from"],
            exclude_cols=["dp_record_id"],
        )
        expected_df = pd.DataFrame(expected, columns=actual_df.columns)

        self.assert_df(actual_df, expected_df, output_file_name=output_file_name)

    def scd2_merge_as_test2(
        self,
        ctx,
        test_step,
        dp_ts_list: list,
        current_ts_list: list,
        expected=None,
        output_file_name: str = None,
        test_description: str = None,
        test_after_description: str = None,
        perform_merge_op: bool = True,
        use_logical_delete_for_source_table: bool = False,
        logical_delete_expression: Optional[str] = None,
        check_physical_delete_against_source_table: bool = True,
        display_result: bool = True,
        show_input_to_merge: bool = True,
        cols_bks: list = ["id"],
        table_shape: str = "flat",
    ):
        render_data(f"## Test Step {test_step}", output_file_name=output_file_name)
        render_data(test_description, output_file_name=output_file_name)

        df_dim_before = self.get_table_data(
            ctx, SCD2Table.SCD2, order_by_cols=cols_bks + ["dp_ts_from"]
        )
        df_raw = self.get_table_data(
            ctx, SCD2Table.RAW, order_by_cols=["dp_loaded_at"] + cols_bks
        )
        render_table(
            df_raw,
            title=f"Raw Table `{RAW_TABLE_NAME}`",
            output_file_name=output_file_name,
        )

        strategy = self._make_strategy(ctx, cols_bks=cols_bks, table_shape=table_shape,
                                       use_logical_delete_for_source_table=use_logical_delete_for_source_table,
                                       logical_delete_expression=logical_delete_expression,
                                       check_physical_delete_against_source_table=check_physical_delete_against_source_table,
                                       perform_merge_op=perform_merge_op)
        for idx, dp_ts in enumerate(dp_ts_list):
            strategy.merge_into_scd2_table(
                dp_ts=dp_ts_list[idx],
                current_ts=current_ts_list[idx],
                show_input_to_merge=show_input_to_merge,
                output_file_name=output_file_name,
            )

        if display_result:
            df = self.get_table_data(
                ctx, SCD2Table.SCD2, order_by_cols=cols_bks + ["dp_ts_from"]
            )
            df_colored = diff_with_color(
                df_dim_before,
                df,
                index_cols=["dp_record_id"],
                sort_cols=cols_bks + ["dp_ts_from"],
            )
            render_table(
                df_colored,
                title=f"Dimensional Table `{SCD2_TABLE_NAME}`",
                decscription=test_after_description,
                exclude_cols=EXCLUDE_COLS,
                output_file_name=output_file_name,
            )
            render_data(test_after_description, output_file_name=output_file_name)

        actual_df = self.get_table_data(
            ctx,
            SCD2Table.SCD2,
            order_by_cols=cols_bks + ["dp_ts_from"],
            exclude_cols=["dp_record_id"],
        )
        expected_df = pd.DataFrame(expected, columns=actual_df.columns)

        self.assert_df(actual_df, expected_df, output_file_name=output_file_name)

    def scd2_sel_as_test(
        self,
        ctx,
        sel_stmt: str,
        expected=None,
        output_file_name: str = None,
        test_description: str = None,
        test_after_description: str = None,
        perform_merge_op: bool = True,
        display_result: bool = True,
        show_input_to_merge: bool = True,
    ):
        actual_df = self.execute_select(sel_stmt, ctx)

        if display_result:
            render_data("### Perform Test", output_file_name=output_file_name)
            render_data(test_description, output_file_name=output_file_name)
            render_data(f"\n\n`{sel_stmt}`\n", output_file_name=output_file_name)
            render_table(
                actual_df,
                title=f"Dimensional Table `{SCD2_TABLE_NAME}`",
                output_file_name=output_file_name,
            )
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
    STRATEGY = "TRINO"

    COLS_WITH_TYPE_STRUCT = [
        "user_info ROW(first_name VARCHAR, last_name VARCHAR, city VARCHAR, email VARCHAR)", 
    ]
    COLS_WITH_TYPE_MAP = [
        "user_info MAP(VARCHAR, VARCHAR)", 
    ]
    COLS_WITH_TYPE_ARRAY = [
        "user_info ARRAY(VARCHAR)", 
    ]
    COLS_WITH_TYPE_JSON = [
        "user_info JSON", 
    ]
    COLS_WITH_TYPE_FLAT = [
        "first_name VARCHAR", 
        "last_name VARCHAR", 
        "city VARCHAR", 
        "email VARCHAR", 
    ]
    COLS_WITH_TYPE_VARIOUS = [
        # Numeric
        "col_tinyint   TINYINT",
        "col_smallint  SMALLINT",
        "col_int       INTEGER",
        "col_bigint    BIGINT",
        "col_real      REAL",
        "col_double    DOUBLE",
        "col_decimal   DECIMAL(18, 4)",

        # String
        "col_varchar   VARCHAR",
        "col_varchar_n VARCHAR(255)",
        "col_char      CHAR(10)",

        # Boolean
        "col_boolean   BOOLEAN",

        # Date / Time
        "col_date                DATE",
        "col_timestamp           TIMESTAMP(6)",
        "col_timestamp_tz        TIMESTAMP(6) WITH TIME ZONE",
        #"col_time                TIME(6)",
        #"col_interval_ym         INTERVAL YEAR TO MONTH",
        #"col_interval_ds         INTERVAL DAY TO SECOND",

        # Binary
        #"col_varbinary  VARBINARY",
    ]   
    
    def _cols_with_type(self, table_shape: str = "flat") -> list:
        if table_shape == "flat":
            return self.COLS_WITH_TYPE_FLAT
        elif table_shape == "struct":
            return self.COLS_WITH_TYPE_STRUCT
        elif table_shape == "map":
            return self.COLS_WITH_TYPE_MAP
        elif table_shape == "array":
            return self.COLS_WITH_TYPE_ARRAY
# json not yet supported by Iceberg
#        elif table_shape == "json":
#            return self.COLS_WITH_TYPE_JSON                
        elif table_shape == "various":
            return self.COLS_WITH_TYPE_VARIOUS
        else:
            raise ValueError(f"Unknown table form: {table_shape}")

    def _make_strategy(self, ctx, cols_bks: list = ["id"], cols_bks_with_type: list = ["id INT"], table_shape: str = "flat",
                       use_logical_delete_for_source_table: bool = False, logical_delete_expression: Optional[str] = None,
                       check_physical_delete_against_source_table: bool = True,
                       perform_merge_op: bool = True, col_dp_ts: str = "dp_ts_from", col_dp_ts_filter: str = "dp_loaded_at"):
        return TrinoSCD2Strategy(
            ctx.conn,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
            source_table_name=RAW_TABLE_NAME,
            scd2_table_name=SCD2_TABLE_NAME,
            cols_bks=cols_bks,
            cols_val=[col.split()[0] for col in self._cols_with_type(table_shape)],
            use_logical_delete_for_source_table=use_logical_delete_for_source_table,
            logical_delete_expression=logical_delete_expression,
            check_physical_delete_against_source_table=check_physical_delete_against_source_table,
            perform_merge_op=perform_merge_op,
            col_dp_valid_from="dp_ts_from",
            col_dp_valid_to="dp_ts_to",
            col_dp_created_at="dp_load_ts",
            col_dp_replaced_at="dp_replace_ts",            
            col_dp_ts=col_dp_ts,
            col_dp_ts_filter=col_dp_ts_filter,
        )
    
    def _create_scd2_table_trino(self, ctx, cols_bks_with_type: list, table_shape: str = "flat") -> None:
        cols_with_type = self._cols_with_type(table_shape)
        
        fqn = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{SCD2_TABLE_NAME}"
        cursor = ctx.conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {fqn}")
        SCD2Strategy.delete_s3_location(
            ctx.s3_client, S3_WAREHOUSE_BUCKET,
            f"{S3_WAREHOUSE_PREFIX}/{TRINO_SCHEMA}/{SCD2_TABLE_NAME}",
        )
        pk_str = ", ".join(cols_bks_with_type)
        cols_str = ", ".join(cols_with_type)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {fqn} (
                dp_record_id VARCHAR,
                {pk_str},
                {cols_str},
                dp_ts_from TIMESTAMP,
                dp_ts_to TIMESTAMP,
                dp_is_active BOOLEAN,
                dp_is_latest BOOLEAN,
                dp_load_ts TIMESTAMP,
                dp_replace_ts TIMESTAMP,
                dp_record_hash VARCHAR
            )
            WITH (
                partitioning = ARRAY['dp_ts_from'],
                sorted_by = ARRAY[],
                location = 's3a://{S3_WAREHOUSE_BUCKET}/{S3_WAREHOUSE_PREFIX}/{TRINO_SCHEMA}/{SCD2_TABLE_NAME}'
            )
        """)
        logger.info(f"Dimension table {fqn} created successfully.")   


    def get_strategy_name(self):
        return self.STRATEGY


    def create_raw_table(self, ctx, cols_bks_with_type: list = ["id INT"], table_shape: str = "flat"):
        cols_with_type = self._cols_with_type(table_shape)

        cursor = ctx.conn.cursor()

        drop_table_sql = (
            f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}"
        )
        cursor.execute(drop_table_sql)
        logger.debug(f"Table {RAW_TABLE_NAME} dropped successfully (if it existed).")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME} (
            {", ".join(cols_bks_with_type)},
            {", ".join(cols_with_type)},
            status VARCHAR,
            dp_ts_from TIMESTAMP,
            dp_loaded_at TIMESTAMP
        )
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['dp_loaded_at']
        )
        """
        logger.info(f"Creating raw table with SQL:\n{create_table_sql}")
        cursor.execute(create_table_sql)
        logger.debug(
            f"Table {RAW_TABLE_NAME} created successfully (or already exists)."
        )

    def create_scd2_table_for_test(self, ctx, cols_bks_with_type: list = ["id INT"], table_shape: str = "flat") -> None:
        self._create_scd2_table_trino(ctx, cols_bks_with_type, table_shape)

    def _execute_insert(self, ins_stmt: str, ctx):
        cursor = ctx.conn.cursor()
        cursor.execute(ins_stmt)

    def execute_select(self, sel_stmt: str, ctx) -> pd.DataFrame:
        df = pd.read_sql_query(sel_stmt, ctx.conn)
        return df


class SparkTestCommons(TestCommonsBase):
    STRATEGY = "SPARK"

    COLS_WITH_TYPE_FLAT = [
        "first_name STRING",
        "last_name STRING",
        "city STRING",
        "email STRING",
    ]
    COLS_WITH_TYPE_STRUCT = [
        "user_info STRUCT<first_name: STRING, last_name: STRING, city: STRING, email: STRING>",
    ]
    COLS_WITH_TYPE_MAP = [
        "user_info MAP<STRING, STRING>", 
    ]
    COLS_WITH_TYPE_ARRAY = [
        "user_info ARRAY<STRING>", 
    ]
    COLS_WITH_TYPE_JSON = [
        "user_info JSON", 
    ]    
    COLS_WITH_TYPE_VARIOUS = [    
        # Numeric
        "col_tinyint   TINYINT",
        "col_smallint  SMALLINT",
        "col_int       INT",
        "col_bigint    BIGINT",
        "col_float     FLOAT",
        "col_double    DOUBLE",
        "col_decimal   DECIMAL(18, 4)",

        # String
        "col_string    STRING",
        "col_varchar_n VARCHAR(255)",
        "col_char      CHAR(10)",

        # Boolean
        "col_boolean   BOOLEAN",

        # Date / Time
        "col_date          DATE",
        "col_timestamp     TIMESTAMP",
        "col_timestamp_ntz TIMESTAMP_NTZ",

        # Binary
        #"col_binary    BINARY",
    ]

    def _cols_with_type(self, table_shape: str = "flat") -> list:
        if table_shape == "flat":
            return self.COLS_WITH_TYPE_FLAT
        elif table_shape == "struct":
            return self.COLS_WITH_TYPE_STRUCT
        elif table_shape == "map":
            return self.COLS_WITH_TYPE_MAP
        elif table_shape == "array":
            return self.COLS_WITH_TYPE_ARRAY
# json not yet supported by Iceberg
#        elif table_shape == "json":
#            return self.COLS_WITH_TYPE_JSON                    
        elif table_shape == "various":
            return self.COLS_WITH_TYPE_VARIOUS
        else:
            raise ValueError(f"Unknown table form: {table_shape}")

    def _make_strategy(self, ctx, cols_bks: list = ["id"], cols_bks_with_type: list = ["id INT"], table_shape: str = "flat",
                       use_logical_delete_for_source_table: bool = False, logical_delete_expression: Optional[str] = None,
                       check_physical_delete_against_source_table: bool = True, 
                       perform_merge_op: bool = True, col_dp_ts: str = "dp_ts_from", col_dp_ts_filter: str = "dp_loaded_at"):
        return SparkSCD2Strategy(
            ctx.spark,
            database="default",
            source_table_name=RAW_TABLE_NAME,
            scd2_table_name=SCD2_TABLE_NAME,
            cols_bks=cols_bks,
            cols_val=[col.split()[0] for col in self._cols_with_type(table_shape)],
            use_logical_delete_for_source_table=use_logical_delete_for_source_table,
            logical_delete_expression=logical_delete_expression,
            check_physical_delete_against_source_table=check_physical_delete_against_source_table,
            perform_merge_op=perform_merge_op,
            col_dp_valid_from="dp_ts_from",
            col_dp_valid_to="dp_ts_to",
            col_dp_created_at="dp_load_ts",
            col_dp_replaced_at="dp_replace_ts",  
            col_dp_ts=col_dp_ts,
            col_dp_ts_filter=col_dp_ts_filter,
        )

    def _create_scd2_table_spark(self, ctx, cols_bks_with_type: list, table_shape: str = "flat") -> None:
        """Shared Spark/PySpark implementation for creating the SCD2 dimension table."""
        cols_with_type = self._cols_with_type(table_shape)
        
        fqn = f"default.{SCD2_TABLE_NAME}"
        ctx.spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        SCD2Strategy.delete_s3_location(
            ctx.s3_client, S3_WAREHOUSE_BUCKET,
            f"{S3_WAREHOUSE_PREFIX}/default/{SCD2_TABLE_NAME}",
        )
        pk_str = ", ".join(cols_bks_with_type)
        cols_str = ", ".join(cols_with_type)
        ctx.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {fqn} (
                dp_record_id STRING,
                {pk_str},
                {cols_str},
                dp_ts_from TIMESTAMP,
                dp_ts_to TIMESTAMP,
                dp_is_active BOOLEAN,
                dp_is_latest BOOLEAN,
                dp_load_ts TIMESTAMP,
                dp_replace_ts TIMESTAMP,
                dp_record_hash STRING
            )
            USING ICEBERG
            PARTITIONED BY (dp_ts_from)
            LOCATION 's3a://{S3_WAREHOUSE_BUCKET}/{S3_WAREHOUSE_PREFIX}/default/{SCD2_TABLE_NAME}'
        """)
        logger.info(f"Dimension table {fqn} created successfully.")

    def get_strategy_name(self):
        return self.STRATEGY

    def create_raw_table(self, ctx, cols_bks_with_type: list = ["id INT"], table_shape: str = "flat"):
        cols_with_type = self._cols_with_type(table_shape)

        drop_table_sql = f"DROP TABLE IF EXISTS default.{RAW_TABLE_NAME}"
        ctx.spark.sql(drop_table_sql)
        logger.debug(f"Table {RAW_TABLE_NAME} dropped successfully (if it existed).")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS default.{RAW_TABLE_NAME} (
            {", ".join(cols_bks_with_type)},
            {", ".join(cols_with_type)},
            status STRING,
            dp_ts_from TIMESTAMP,
            dp_loaded_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (dp_loaded_at)
        LOCATION 's3a://{S3_WAREHOUSE_BUCKET}/{S3_WAREHOUSE_PREFIX}/default/{RAW_TABLE_NAME}'
        """

        ctx.spark.sql(create_table_sql)
        logger.debug(
            f"Table {RAW_TABLE_NAME} created successfully (or already exists)."
        )

    def create_scd2_table_for_test(self, ctx, cols_bks_with_type: list = ["id INT"], table_shape: str = "flat") -> None:
        self._create_scd2_table_spark(ctx, cols_bks_with_type, table_shape)

    def _execute_insert(self, ins_stmt, ctx):
        ctx.spark.sql(ins_stmt)

    def execute_select(self, sel_stmt: str, ctx) -> pd.DataFrame:
        df = ctx.spark.sql(sel_stmt).toPandas()
        return df

class PySparkTestCommons(SparkTestCommons):
    STRATEGY = "PYSPARK"

    COLS_WITH_TYPE_SPARK = [
        "first_name STRING",
        "last_name STRING",
        "city STRING",
        "email STRING",
    ]
    COLS_WITH_TYPE_STRUCT = [
        "user_info STRUCT<first_name: STRING, last_name: STRING, city: STRING, email: STRING>",
    ]
    COLS_WITH_TYPE_MAP = [
        "user_info MAP<STRING, STRING>", 
    ]
    COLS_WITH_TYPE_ARRAY = [
        "user_info ARRAY<STRING>", 
    ]
    COLS_WITH_TYPE_JSON = [
        "user_info JSON", 
    ]    
    COLS_WITH_TYPE_VARIOUS = [    
        # Numeric
        "col_tinyint   TINYINT",
        "col_smallint  SMALLINT",
        "col_int       INT",
        "col_bigint    BIGINT",
        "col_float     FLOAT",
        "col_double    DOUBLE",
        "col_decimal   DECIMAL(18, 4)",

        # String
        "col_string    STRING",
        "col_varchar_n VARCHAR(255)",
        "col_char      CHAR(10)",

        # Boolean
        "col_boolean   BOOLEAN",

        # Date / Time
        "col_date          DATE",
        "col_timestamp     TIMESTAMP",
        "col_timestamp_ntz TIMESTAMP_NTZ",

        # Binary
        #"col_binary    BINARY"
        ]

    def _cols_with_type(self, table_shape: str = "flat") -> list:
        if table_shape == "flat":
            return self.COLS_WITH_TYPE_FLAT
        elif table_shape == "struct":
            return self.COLS_WITH_TYPE_STRUCT
        elif table_shape == "map":
            return self.COLS_WITH_TYPE_MAP
        elif table_shape == "array":
            return self.COLS_WITH_TYPE_ARRAY
# json not yet supported by Iceberg
#        elif table_shape == "json":
#            return self.COLS_WITH_TYPE_JSON                
        elif table_shape == "various":
            return self.COLS_WITH_TYPE_VARIOUS
        else:
            raise ValueError(f"Unknown table form: {table_shape}")    

    def _make_strategy(self, ctx, cols_bks: list = ["id"], cols_bks_with_type: list = ["id INT"], table_shape: str = "flat",
                       use_logical_delete_for_source_table: bool = False, logical_delete_expression: Optional[str] = None, 
                       check_physical_delete_against_source_table: bool = True, perform_merge_op: bool = True, col_dp_ts: str = "dp_ts_from", col_dp_ts_filter: str = "dp_loaded_at"):
        
        return PySparkSCD2Strategy(
            ctx.spark,
            database="default",
            source_table_name=RAW_TABLE_NAME,
            source_table_df=ctx.spark.table("default." + RAW_TABLE_NAME),
            scd2_table_name=SCD2_TABLE_NAME,
            cols_bks=cols_bks,
            cols_val=[col.split()[0] for col in self._cols_with_type(table_shape)],
            use_logical_delete_for_source_table=use_logical_delete_for_source_table,
            logical_delete_expression=logical_delete_expression,
            check_physical_delete_against_source_table=check_physical_delete_against_source_table,
            perform_merge_op=perform_merge_op,
            col_dp_valid_from="dp_ts_from",
            col_dp_valid_to="dp_ts_to",
            col_dp_created_at="dp_load_ts",
            col_dp_replaced_at="dp_replace_ts",
            col_dp_ts=col_dp_ts,
            col_dp_ts_filter=col_dp_ts_filter,
        )
    
    def get_strategy_name(self):
        return self.STRATEGY

# ---------------------------------------------------------------------------
# Active implementation — switch here to run tests against a different engine
# ---------------------------------------------------------------------------
# Choose one of: "TRINO", "SPARK", "PYSPARK"
TEST_ENGINE = os.environ.get("TEST_ENGINE", "PYSPARK").upper()

_ENGINES: dict[str, type[TestCommonsBase]] = {
    "TRINO": TrinoTestCommons,
    "SPARK": SparkTestCommons,
    "PYSPARK": PySparkTestCommons,
}

if TEST_ENGINE not in _ENGINES:
    raise ValueError(f"Unknown TEST_ENGINE={TEST_ENGINE!r}. Choose from: {list(_ENGINES)}")

_impl: TestCommonsBase = _ENGINES[TEST_ENGINE]()

# Re-export COLS_WITH_TYPE so test files can import it directly from commons
COLS_WITH_TYPE_FLAT = _impl.COLS_WITH_TYPE_FLAT
COLS_WITH_TYPE_STRUCT = _impl.COLS_WITH_TYPE_STRUCT



# ---------------------------------------------------------------------------
# Module-level wrappers — kept for backward compatibility with test imports
# ---------------------------------------------------------------------------


def get_strategy_name():
    return _impl.get_strategy_name()


def get_table_data(
    ctx,
    table: SCD2Table,
    iceberg_meta_tablename: str = None,
    exclude_cols: list = [],
    order_by_cols: list = [],
    for_version: str = None,
):
    return _impl.get_table_data(
        ctx,
        table=table,
        iceberg_meta_tablename=iceberg_meta_tablename,
        exclude_cols=exclude_cols,
        order_by_cols=order_by_cols,
        for_version=for_version,
    )


def source_table_fqn(ctx, iceberg_meta_tablename: str = None):
    return _impl.source_table_fqn(ctx, iceberg_meta_tablename=iceberg_meta_tablename)


def scd2_table_fqn(ctx, iceberg_meta_tablename: str = None):
    return _impl.scd2_table_fqn(ctx, iceberg_meta_tablename=iceberg_meta_tablename)


def scd2_intermediary_table_fqn(ctx, iceberg_meta_tablename: str = None):
    return _impl.scd2_intermediary_table_fqn(
        ctx, iceberg_meta_tablename=iceberg_meta_tablename
    )

def create_raw_table(ctx, cols_bks_with_type: list = ["id INT"], table_shape: str = "flat"):
    return _impl.create_raw_table(ctx, cols_bks_with_type=cols_bks_with_type, table_shape=table_shape)


def create_scd2_table_for_test(ctx, cols_bks_with_type: list = ["id INT"], table_shape: str = "flat"):
    return _impl.create_scd2_table_for_test(
        ctx, cols_bks_with_type=cols_bks_with_type, table_shape=table_shape
    )

def execute_select(sel_stmt: str, ctx) -> pd.DataFrame:
    return _impl.execute_select(sel_stmt, ctx)

def insert_as_preparation(ctx, ins_stmts: list):
    return _impl.insert_as_preparation(ctx, ins_stmts=ins_stmts)


def scd2_merge_as_preparation(
    ctx,
    ins_stmts: list,
    dp_ts_list: list,
    current_ts_list: list,
    perform_merge_op: bool = True,
    use_logical_delete_for_source_table: bool = False,
    logical_delete_expression: Optional[str] = None,
    check_physical_delete_against_source_table: bool = True,
    display_result: bool = True,
    expected=None,
    output_file_name: str = None,
    test_description: str = None,
    cols_bks: list = ["id"],
):
    _impl.scd2_merge_as_preparation(
        ctx,
        ins_stmts=ins_stmts,
        dp_ts_list=dp_ts_list,
        current_ts_list=current_ts_list,
        perform_merge_op=perform_merge_op,
        use_logical_delete_for_source_table=use_logical_delete_for_source_table,
        logical_delete_expression=logical_delete_expression,
        check_physical_delete_against_source_table=check_physical_delete_against_source_table,
        display_result=display_result,
        expected=expected,
        output_file_name=output_file_name,
        test_description=test_description,
        cols_bks=cols_bks,
    )


def scd2_merge_as_test(
    ctx,
    test_step: int,
    ins_stmt: str,
    dp_ts: datetime,
    current_ts: datetime,
    expected=None,
    output_file_name: str = None,
    test_description: str = None,
    test_after_description: str = None,
    perform_merge_op: bool = True,
    use_logical_delete_for_source_table: bool = False,
    logical_delete_expression: Optional[str] = None,
    check_physical_delete_against_source_table: bool = True,
    display_result: bool = True,
    show_input_to_merge: bool = True,
    cols_bks: list = ["id"],
    table_shape: str = "flat",
):
    _impl.scd2_merge_as_test(
        ctx,
        test_step=test_step,
        ins_stmt=ins_stmt,
        dp_ts=dp_ts,
        current_ts=current_ts,
        expected=expected,
        output_file_name=output_file_name,
        test_description=test_description,
        test_after_description=test_after_description,
        perform_merge_op=perform_merge_op,
        use_logical_delete_for_source_table=use_logical_delete_for_source_table,
        logical_delete_expression=logical_delete_expression,
        check_physical_delete_against_source_table=check_physical_delete_against_source_table,
        display_result=display_result,
        show_input_to_merge=show_input_to_merge,
        cols_bks=cols_bks,
        table_shape=table_shape,
    )

def scd2_merge_as_test2(
    ctx,
    test_step,
    dp_ts_list: list,
    current_ts_list: list,
    expected=None,
    output_file_name: str = None,
    test_description: str = None,
    test_after_description: str = None,
    perform_merge_op: bool = True,
    use_logical_delete_for_source_table: bool = False,
    logical_delete_expression: Optional[str] = None,
    check_physical_delete_against_source_table: bool = True,
    display_result: bool = True,
    show_input_to_merge: bool = True,
    cols_bks: list = ["id"],
):
    _impl.scd2_merge_as_test2(
        ctx,
        test_step=test_step,
        dp_ts_list=dp_ts_list,
        current_ts_list=current_ts_list,
        expected=expected,
        output_file_name=output_file_name,
        test_description=test_description,
        test_after_description=test_after_description,
        perform_merge_op=perform_merge_op,
        use_logical_delete_for_source_table=use_logical_delete_for_source_table,
        logical_delete_expression=logical_delete_expression,
        check_physical_delete_against_source_table=check_physical_delete_against_source_table,
        display_result=display_result,
        show_input_to_merge=show_input_to_merge,
        cols_bks=cols_bks,
    )

def scd2_merge_as_test_return_as_df(
    ctx,
    test_step: int,
    ins_stmt: str,
    dp_ts: datetime,
    current_ts: datetime,
    expected=None,
    output_file_name: str = None,
    test_description: str = None,
    test_after_description: str = None,
    perform_merge_op: bool = True,
    use_logical_delete_for_source_table: bool = False,
    logical_delete_expression: Optional[str] = None,
    check_physical_delete_against_source_table: bool = True,
    display_result: bool = True,
    show_input_to_merge: bool = True,
    cols_bks: list = ["id"],
) -> DataFrame:
    return _impl.scd2_merge_as_test_return_as_df(
        ctx,
        test_step=test_step,
        ins_stmt=ins_stmt,
        dp_ts=dp_ts,
        current_ts=current_ts,
        expected=expected,
        output_file_name=output_file_name,
        test_description=test_description,
        test_after_description=test_after_description,
        perform_merge_op=perform_merge_op,
        use_logical_delete_for_source_table=use_logical_delete_for_source_table,
        logical_delete_expression=logical_delete_expression,
        check_physical_delete_against_source_table=check_physical_delete_against_source_table,
        display_result=display_result,
        show_input_to_merge=show_input_to_merge,
        cols_bks=cols_bks,
    )

def scd2_sel_as_test(
    ctx,
    sel_stmt: str,
    expected=None,
    output_file_name: str = None,
    test_description: str = None,
    test_after_description: str = None,
    perform_merge_op: bool = True,
    display_result: bool = True,
    show_input_to_merge: bool = True,
):
    return _impl.scd2_sel_as_test(
        ctx,
        sel_stmt=sel_stmt,
        expected=expected,
        output_file_name=output_file_name,
        test_description=test_description,
        test_after_description=test_after_description,
        perform_merge_op=perform_merge_op,
        display_result=display_result,
        show_input_to_merge=show_input_to_merge,
    )


def optimize_table(ctx, table_name: str) -> None:
    return _impl.optimize_table(ctx, table_name)


def analyze_table(ctx, table_name: str) -> None:
    return _impl.analyze_table(ctx, table_name)
