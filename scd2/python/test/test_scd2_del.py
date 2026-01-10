import sys
import os
import logging
import pandas as pd
import uuid
import boto3
import trino
from trino.auth import BasicAuthentication
from tabulate import tabulate
import pytest
from datetime import date, timedelta, datetime
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_table, get_table_data
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from scd2 import merge_into_dim_table, create_dim_table
from constants import MAX_TS
from commons import DIM_TABLE_NAME, RAW_TABLE_NAME, SCD2_VIEW_NAME, EXCLUDE_COLS, COLS_WITH_TYPE, run_scd2_merge_test

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
    auth=BasicAuthentication(
        TRINO_USER,
        TRINO_PASSWORD
    ) if TRINO_PASSWORD else None,
    verify=False  # Disable SSL verification for self-signed certificates,
)

def create_raw_table():
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

load_ts_1= datetime.strptime('2026-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_1 = datetime.strptime('2026-01-02 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_2 = datetime.strptime('2026-01-05 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_2 = datetime.strptime('2026-01-06 00:00:00', '%Y-%m-%d %H:%M:%S')


#@pytest.fixture(autouse=True, scope="session")
#def setup_data(request):
#    create_raw_table()
#    create_dim_table(conn, TRINO_CATALOG, TRINO_SCHEMA, "{DIM_TABLE_NAME}", s3_warehouse_bucket=S3_WAREHOUSE_BUCKET, s3_warehouse_prefix=S3_WAREHOUSE_PREFIX, pk_col_with_type="id INT", cols_with_type=cols_with_type, partition_cols=["dp_valid_from"], sort_cols=[])
#    yield
#    logger.info("Finished all tests")


def test_step_1():
    logger.info("-------------------------------- Test Step 1 --------------------------------")

    create_raw_table()
    create_dim_table(conn, TRINO_CATALOG, TRINO_SCHEMA, DIM_TABLE_NAME, s3_warehouse_bucket=S3_WAREHOUSE_BUCKET, s3_warehouse_prefix=S3_WAREHOUSE_PREFIX, pk_col_with_type="id INT", cols_with_type=COLS_WITH_TYPE, partition_cols=["dp_valid_from"], sort_cols=[])

    # --- Insert statement (batch 1) ---
    insert_sql_1 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}'),
                (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}')
        ) AS t (
            id,
            first_name,
            last_name,
            city,
            email,
            status,
            dp_exported_at
        )
    """

    expected = [
        (1, "Alice", "Meyer", "Zurich", "alice.meyer@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, current_ts_1, MAX_TS,
        "NEW", "FF118EED04F8A2D0133E79435F7BC3CEBC0011D256A07FE02953CD12B3E29E51"),

        (2, "Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, current_ts_1, MAX_TS,
        "NEW", "68844625A41E2D2540D4A17FBC7B51B3733C95FC58817DA05765F111F4F659CE"),

        (3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, current_ts_1, MAX_TS,
        "NEW", "67A87A1E14991AF623E8AC26518B9BB757E481E9B47AE9CBC728833FDDCEF86E"),
    ]

    # run test
    run_scd2_merge_test(conn, insert_sql_1, load_ts_1, current_ts_1, expected)

def test_step_2():
    logger.info("-------------------------------- Test Step 2 --------------------------------")

    cursor = conn.cursor()

    # --- Insert statement (batch 2) ---
    insert_sql_2 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}')
        ) AS t (
            id,
            first_name,
            last_name,
            city,
            email,
            status,
            dp_exported_at
        )
    """

    expected = [
        (1, "Alice", "Meyer", "Zurich", "alice.meyer@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, current_ts_1, MAX_TS,
        "NEW", "FF118EED04F8A2D0133E79435F7BC3CEBC0011D256A07FE02953CD12B3E29E51"),

        (2, "Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, current_ts_1, MAX_TS,
        "NEW", "68844625A41E2D2540D4A17FBC7B51B3733C95FC58817DA05765F111F4F659CE"),

        (3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
        load_ts_1, load_ts_2 - timedelta(seconds=1), False, True,
        current_ts_1, current_ts_1, current_ts_2,
        "DELETED", "67A87A1E14991AF623E8AC26518B9BB757E481E9B47AE9CBC728833FDDCEF86E"),
    ]

    # run test
    run_scd2_merge_test(conn, insert_sql_2, load_ts_2, current_ts_2, expected)



