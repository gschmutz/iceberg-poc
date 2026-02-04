import pandas as pd
import sys
import os
import logging
import trino
import numpy as np

from datetime import date, timedelta, datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_table, render_data, get_table_data, diff_with_color
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from scd2 import merge_into_dim_table 
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
LOAD_TS_COL="dp_exported_at"

def init_trino_connection():
    if TRINO_USE_SSL:
        http_scheme = "https"
    else:
        http_scheme = "http"

    # Construct connection URLs
    conn = trino.dbapi.connect(
        host=f"{TRINO_HOST}",
        port=int(TRINO_PORT),
        user=f"{TRINO_USER}",
        catalog=f"{TRINO_CATALOG}",
        schema=f"{TRINO_SCHEMA}",
        http_scheme=http_scheme,
    )
    return conn

def create_raw_table(conn):
    cursor = conn.cursor()

    drop_table_sql = f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}"
    cursor.execute(drop_table_sql)
    logger.debug(f"Table {RAW_TABLE_NAME} dropped successfully (if it existed).")
    

    # --- 1. Create Iceberg table ---
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME} (
        id INT,
        first_name VARCHAR,
        last_name VARCHAR,
        city VARCHAR,
        email VARCHAR,
        status VARCHAR,
        dp_exported_at TIMESTAMP
    )
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['dp_exported_at']
    )
    """

    cursor.execute(create_table_sql)
    logger.debug(f"Table {RAW_TABLE_NAME} created successfully (or already exists).")

def scd2_merge_as_preparation(conn, ins_stmts: list, load_ts: list, current_ts: list, perform_merge_op: bool = True, display_result: bool = True, output_file_name:str=None):

    # --- Prepare raw data ---
    cursor = conn.cursor()

    for idx, ins_stmt in enumerate(ins_stmts):
        cursor.execute(ins_stmt)

        # run dimensional merge
        merge_into_dim_table(
            conn=conn,
            trino_catalog=TRINO_CATALOG,
            trino_schema=TRINO_SCHEMA,
            raw_table_name=RAW_TABLE_NAME,
            dim_table_name=DIM_TABLE_NAME,
            scd2_view_name=SCD2_VIEW_NAME,
            load_ts=load_ts[idx],
            load_ts_col="dp_exported_at",
            pk_col="id",
            cols_with_type=COLS_WITH_TYPE,
            current_ts=current_ts[idx],
            perform_merge_op=perform_merge_op,
            show_input_to_merge=False
        )
    
    render_data(f"### Perform Preparation", output_file_name=output_file_name)

    df_raw = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}", order_by_cols=["dp_exported_at", "id"])
    render_table(df_raw, title=f"Raw Table `{RAW_TABLE_NAME}`", output_file_name=output_file_name)

    df = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{DIM_TABLE_NAME}", order_by_cols=["id", "dp_valid_from"])
    render_table(df, title=f"Dimensional Table `{DIM_TABLE_NAME}`", exclude_cols=EXCLUDE_COLS, output_file_name=output_file_name)

def scd2_merge_as_test(conn, test_step: int, ins_stmt: str, load_ts: datetime, current_ts: datetime, expected = None, output_file_name:str=None, test_description:str=None, test_after_description:str=None, perform_merge_op: bool = True, display_result: bool = True, show_input_to_merge: bool = True):

    # --- Prepare raw data ---
    cursor = conn.cursor()
    cursor.execute(ins_stmt)

    render_data(f"## Test Step {test_step}", output_file_name=output_file_name)
    render_data(test_description, output_file_name=output_file_name)

    df_dim_before = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{DIM_TABLE_NAME}", order_by_cols=["id", "dp_valid_from"])
    df_raw = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}", order_by_cols=["dp_exported_at", "id"])
    render_table(df_raw, title=f"Raw Table `{RAW_TABLE_NAME}`", output_file_name=output_file_name)

    # run dimensional merge
    merge_into_dim_table(
        conn=conn,
        trino_catalog=TRINO_CATALOG,
        trino_schema=TRINO_SCHEMA,
        raw_table_name=RAW_TABLE_NAME,
        dim_table_name=DIM_TABLE_NAME,
        scd2_view_name=SCD2_VIEW_NAME,
        load_ts=load_ts,
        load_ts_col="dp_exported_at",
        pk_col="id",
        cols_with_type=COLS_WITH_TYPE,
        current_ts=current_ts,
        perform_merge_op=perform_merge_op,
        show_input_to_merge=show_input_to_merge,
        output_file_name=output_file_name
    )
    if display_result:
        df = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{DIM_TABLE_NAME}", order_by_cols=["id", "dp_valid_from"])
        df_colored = diff_with_color(df_dim_before, df, index_cols=["id", "dp_valid_from"])    

        render_table(df_colored, title=f"Dimensional Table `{DIM_TABLE_NAME}`", decscription=test_after_description, exclude_cols=EXCLUDE_COLS, output_file_name=output_file_name)
        render_data(test_after_description, output_file_name=output_file_name)

    expected_df = pd.DataFrame(expected, columns=df.columns)

    arr1 = df.to_numpy()
    arr2 = expected_df.to_numpy()
    #print (arr1)
    np.testing.assert_array_equal(arr1, arr2)

def scd2_sel_as_test(conn, sel_stmt: str, expected = None, output_file_name:str=None, test_description:str=None, test_after_description:str=None, perform_merge_op: bool = True, display_result: bool = True, show_input_to_merge: bool = True):
        
    df = pd.read_sql_query(sel_stmt, conn)

    if display_result:
        render_data(f"### Perform Test", output_file_name=output_file_name)
        render_data(test_description, output_file_name=output_file_name)
        render_data(f"\n\n`{sel_stmt}`\n", output_file_name=output_file_name)

        render_table(df, title=f"Dimensional Table `{DIM_TABLE_NAME}`", output_file_name=output_file_name)
        render_data(test_after_description, output_file_name=output_file_name)

    expected_df = pd.DataFrame(expected, columns=df.columns)

    arr1 = df.to_numpy()
    arr2 = expected_df.to_numpy()
    np.testing.assert_array_equal(arr1, arr2)
