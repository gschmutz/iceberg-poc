import pandas as pd
import sys
import os
import logging
import trino
import numpy as np
np.set_printoptions(threshold=np.inf)

from datetime import date, timedelta, datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_table, render_data, get_table_data, diff_with_color
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from scd2_trino import TrinoSCD2Strategy
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

DIM_TABLE_NAME="dim_person"
RAW_TABLE_NAME="raw_person"
SCD2_VIEW_NAME="view_person_scd2"

COLS_WITH_TYPE = [
        "first_name VARCHAR",
        "last_name VARCHAR",
        "city VARCHAR",
        "email VARCHAR",
    ]

EXCLUDE_COLS = ["record_hash","dp_load_timestamp", "change_type"]
LOAD_TS_COL="dp_loaded_at"

def _make_strategy(conn, spark) -> TrinoSCD2Strategy:
    #return SparkSCD2Strategy(spark, database="default")
    return TrinoSCD2Strategy(conn, catalog=TRINO_CATALOG, schema=TRINO_SCHEMA)

def create_raw_table(conn,  pk_columns_with_type: list = ["id INT"]):
    cursor = conn.cursor()

    drop_table_sql = f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}"
    cursor.execute(drop_table_sql)
    logger.debug(f"Table {RAW_TABLE_NAME} dropped successfully (if it existed).")
    

    # --- 1. Create Iceberg table ---
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

def create_dim_table_for_test(conn, spark, pk_columns_with_type: list = ["id INT"]):
    _make_strategy(conn, spark).create_dim_table(DIM_TABLE_NAME, s3_warehouse_bucket=S3_WAREHOUSE_BUCKET, s3_warehouse_prefix=S3_WAREHOUSE_PREFIX, pk_columns_with_type=pk_columns_with_type, cols_with_type=COLS_WITH_TYPE, partition_cols=["dp_ts_from"], sort_cols=[])

def scd2_merge_as_preparation(conn, spark, ins_stmts: list, load_ts_list: list, current_ts_list: list, perform_merge_op: bool = True, use_delta_mode_for_raw_table: bool = False, display_result: bool = True, expected = None, output_file_name:str=None, test_description:str=None, pk_columns: list = ["id"]):

    # --- Prepare raw data ---
    cursor = conn.cursor()
    
    render_data(test_description, output_file_name=output_file_name)
    
    for idx, ins_stmt in enumerate(ins_stmts):
        cursor.execute(ins_stmt)

        print (load_ts_list[idx], current_ts_list[idx])

        # run dimensional merge
        _make_strategy(conn, spark).merge_into_dim_table(
            raw_table_name=RAW_TABLE_NAME,
            dim_table_name=DIM_TABLE_NAME,
            scd2_view_name=SCD2_VIEW_NAME,
            load_ts=load_ts_list[idx],
            load_ts_col="dp_loaded_at",
            pk_columns=pk_columns,
            cols_with_type=COLS_WITH_TYPE,
            current_ts=current_ts_list[idx],
            use_delta_mode_for_raw_table=use_delta_mode_for_raw_table,
            perform_merge_op=perform_merge_op,
            show_input_to_merge=False,
        )
    
    render_data(f"### Perform Preparation", output_file_name=output_file_name)

    df_raw = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}", order_by_cols=["dp_loaded_at"]+pk_columns)
    render_table(df_raw, title=f"Raw Table `{RAW_TABLE_NAME}`", output_file_name=output_file_name)

    if display_result:
        df = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{DIM_TABLE_NAME}", order_by_cols=pk_columns+["dp_ts_from"])
        render_table(df, title=f"Dimensional Table `{DIM_TABLE_NAME}`", exclude_cols=EXCLUDE_COLS, output_file_name=output_file_name)

    if (expected is not None):
        actual_df = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{DIM_TABLE_NAME}", order_by_cols=pk_columns+["dp_ts_from"], exclude_cols="dp_key")
        expected_df = pd.DataFrame(expected, columns=actual_df.columns)
        try:
            pd.testing.assert_frame_equal(actual_df, expected_df, check_dtype=False, check_like=False)
        except AssertionError as e:
            error_msg = f"### Assertion Error\n\n```\n{str(e)}\n```\n"
            render_data(error_msg, output_file_name=output_file_name)
            raise

def scd2_merge_as_test(conn, spark, test_step: int, ins_stmt: str, load_ts: datetime, current_ts: datetime, expected = None, output_file_name:str=None, test_description:str=None, test_after_description:str=None, perform_merge_op: bool = True, use_delta_mode_for_raw_table: bool = False,display_result: bool = True, show_input_to_merge: bool = True, pk_columns: list = ["id"]):

    # --- Prepare raw data ---
    cursor = conn.cursor()
    cursor.execute(ins_stmt)

    render_data(f"## Test Step {test_step}", output_file_name=output_file_name)
    render_data(test_description, output_file_name=output_file_name)

    df_dim_before = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{DIM_TABLE_NAME}", order_by_cols=pk_columns+["dp_ts_from"])
    df_raw = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}", order_by_cols=["dp_loaded_at"]+pk_columns)
    render_table(df_raw, title=f"Raw Table `{RAW_TABLE_NAME}`", output_file_name=output_file_name)

    # run dimensional merge
    _make_strategy(conn, spark).merge_into_dim_table(
        raw_table_name=RAW_TABLE_NAME,
        dim_table_name=DIM_TABLE_NAME,
        scd2_view_name=SCD2_VIEW_NAME,
        load_ts=load_ts,
        load_ts_col="dp_loaded_at",
        pk_columns=pk_columns,
        cols_with_type=COLS_WITH_TYPE,
        current_ts=current_ts,
        use_delta_mode_for_raw_table=use_delta_mode_for_raw_table,
        perform_merge_op=perform_merge_op,
        show_input_to_merge=show_input_to_merge,
        output_file_name=output_file_name,
    )
    if display_result:
        df = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{DIM_TABLE_NAME}", order_by_cols=pk_columns+["dp_ts_from"])
        df_colored = diff_with_color(df_dim_before, df, index_cols=["dp_key"], sort_cols=pk_columns+["dp_ts_from"])    

        render_table(df_colored, title=f"Dimensional Table `{DIM_TABLE_NAME}`", decscription=test_after_description, exclude_cols=EXCLUDE_COLS, output_file_name=output_file_name)
        render_data(test_after_description, output_file_name=output_file_name)

    actual_df = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{DIM_TABLE_NAME}", order_by_cols=pk_columns+["dp_ts_from"], exclude_cols="dp_key")
    expected_df = pd.DataFrame(expected, columns=actual_df.columns)

    #arr1 = actual_df.to_numpy()
    #arr2 = expected_df.to_numpy()
    #np.testing.assert_array_equal(arr1, arr2, verbose=True)
    try:
        pd.testing.assert_frame_equal(actual_df, expected_df, check_dtype=False, check_like=False)
    except AssertionError as e:
        error_msg = f"### Assertion Error\n\n```\n{str(e)}\n```\n"
        render_data(error_msg, output_file_name=output_file_name)
        raise

def scd2_merge_as_test2(conn, spark,test_step, load_ts_list: list, current_ts_list: list, expected = None, output_file_name:str=None, test_description:str=None, test_after_description:str=None, perform_merge_op: bool = True, use_delta_mode_for_raw_table: bool = False,display_result: bool = True, show_input_to_merge: bool = True, pk_columns: list = ["id"]):

    render_data(f"## Test Step {test_step}", output_file_name=output_file_name)
    render_data(test_description, output_file_name=output_file_name)

    df_dim_before = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{DIM_TABLE_NAME}", order_by_cols=pk_columns+["dp_ts_from"])
    df_raw = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}", order_by_cols=["dp_loaded_at"]+pk_columns)
    render_table(df_raw, title=f"Raw Table `{RAW_TABLE_NAME}`", output_file_name=output_file_name)

    strategy = _make_strategy(conn, spark)
    for idx, load_ts in enumerate(load_ts_list):
        strategy.merge_into_dim_table(
            raw_table_name=RAW_TABLE_NAME,
            dim_table_name=DIM_TABLE_NAME,
            scd2_view_name=SCD2_VIEW_NAME,
            load_ts=load_ts_list[idx],
            load_ts_col="dp_loaded_at",
            pk_columns=pk_columns,
            cols_with_type=COLS_WITH_TYPE,
            current_ts=current_ts_list[idx],
            use_delta_mode_for_raw_table=use_delta_mode_for_raw_table,
            perform_merge_op=perform_merge_op,
            show_input_to_merge=show_input_to_merge,
            output_file_name=output_file_name,
        )

    if display_result:
        df = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{DIM_TABLE_NAME}", order_by_cols=pk_columns+["dp_ts_from"])
        df_colored = diff_with_color(df_dim_before, df, index_cols=["dp_key"], sort_cols=pk_columns+["dp_ts_from"])    

        render_table(df_colored, title=f"Dimensional Table `{DIM_TABLE_NAME}`", decscription=test_after_description, exclude_cols=EXCLUDE_COLS, output_file_name=output_file_name)
        render_data(test_after_description, output_file_name=output_file_name)

    actual_df = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{DIM_TABLE_NAME}", order_by_cols=pk_columns+["dp_ts_from"], exclude_cols="dp_key")
    expected_df = pd.DataFrame(expected, columns=actual_df.columns)

    #arr1 = actual_df.to_numpy()
    #arr2 = expected_df.to_numpy()
    #np.testing.assert_array_equal(arr1, arr2, verbose=True)
    pd.testing.assert_frame_equal(actual_df, expected_df, check_dtype=False, check_like=False)

def scd2_sel_as_test(conn, sel_stmt: str, expected = None, output_file_name:str=None, test_description:str=None, test_after_description:str=None, perform_merge_op: bool = True, display_result: bool = True, show_input_to_merge: bool = True):
        
    actual_df = pd.read_sql_query(sel_stmt, conn)

    if display_result:
        render_data(f"### Perform Test", output_file_name=output_file_name)
        render_data(test_description, output_file_name=output_file_name)
        render_data(f"\n\n`{sel_stmt}`\n", output_file_name=output_file_name)

        render_table(actual_df, title=f"Dimensional Table `{DIM_TABLE_NAME}`", output_file_name=output_file_name)
        render_data(test_after_description, output_file_name=output_file_name)

    expected_df = pd.DataFrame(expected, columns=actual_df.columns)

#    arr1 = actual_df.to_numpy()
#    arr2 = expected_df.to_numpy()
#    np.testing.assert_array_equal(arr1, arr2, verbose=True)
    pd.testing.assert_frame_equal(actual_df, expected_df, check_dtype=False, check_like=False)


def optimize_table(conn, spark, table_name: str) -> None:
    strategy = _make_strategy(conn, spark)

    strategy.optimize_table(table_name)

def analyze_table(conn, spark, table_name: str) -> None:
    strategy = _make_strategy(conn, spark)

    strategy.analyze_table(table_name)    