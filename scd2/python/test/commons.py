import pandas as pd
import sys
import os
import logging
from datetime import date, timedelta, datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_table, get_table_data
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

def run_scd2_merge_test(conn, ins_stmt: str, load_ts: datetime, current_ts: datetime, expected):

    # --- Prepare raw data ---
    cursor = conn.cursor()
    cursor.execute(ins_stmt)

    df_raw = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}", order_by_cols=["dp_exported_at", "id"])
    render_table(df_raw)

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
        current_ts=current_ts
    )
    df = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{DIM_TABLE_NAME}", order_by_cols=["id", "dp_valid_from"])
    render_table(df, exclude_cols=EXCLUDE_COLS)

    expected_df = pd.DataFrame(expected, columns=df.columns)
    pd.testing.assert_frame_equal(df, expected_df)
