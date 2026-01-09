import sys
import os
import logging
import uuid
import boto3
import trino
from trino.auth import BasicAuthentication
from tabulate import tabulate
import pytest
from datetime import date, timedelta, datetime
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string, execute_with_metrics
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
from scd2 import run_dim_update, create_dim_table 

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

cols_with_type = [
        "first_name VARCHAR",
        "last_name VARCHAR",
        "city VARCHAR",
        "email VARCHAR",
    ]

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

VOLATILE_IDX = {9, 10, 11}
def strip_volatile(row):
    return tuple(
        v for i, v in enumerate(row)
        if i not in VOLATILE_IDX
    )

def normalize_row(row):
    return tuple(
        v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime) else v
        for v in row
    )

def create_raw_table():
    cursor = conn.cursor()

    drop_table_sql = f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.employees"
    cursor.execute(drop_table_sql)
    logger.info("Table 'employees' dropped successfully (if it existed).")

    # --- 1. Create Iceberg table ---
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.employees (
        id INT,
        first_name VARCHAR,
        last_name VARCHAR,
        city VARCHAR,
        email VARCHAR,
        status VARCHAR,
        load_ts DATE
    )
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['load_ts']
    )
    """

    cursor.execute(create_table_sql)
    logger.info("Table 'employees' created successfully (or already exists).")

def select_from_table(table_name: str, order_by_cols: list):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT *
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{table_name}
        ORDER BY {", ".join(order_by_cols)}
    """)

    rows = cursor.fetchall()

    columns = [desc[0] for desc in cursor.description]

    print(tabulate(rows, headers=columns, tablefmt="github", ))
    print("")

    normalized_rows = [normalize_row(r) for r in rows]
    return normalized_rows

load_ts_1 = '2026-01-01'
current_ts_1 = '2026-01-02 00:00:00'

load_ts_2 = '2026-01-05'
current_ts_2 = '2026-01-06 00:00:00'

@pytest.fixture(autouse=True, scope="session")
def setup_data():
    create_raw_table()
    create_dim_table(conn, TRINO_CATALOG, TRINO_SCHEMA, "dim_employees", s3_warehouse_bucket=S3_WAREHOUSE_BUCKET, s3_warehouse_prefix=S3_WAREHOUSE_PREFIX, pk_col_with_type="id INT", cols_with_type=cols_with_type, partition_cols=["dp_valid_from"], sort_cols=[])
    yield
    logger.info("Finished all tests")


def test_step_1():
    cursor = conn.cursor()

    # --- Insert statement (batch 1) ---
    insert_sql_1 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.employees
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', DATE '{load_ts_1}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', DATE '{load_ts_1}'),
                (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', DATE '{load_ts_1}')
        ) AS t (
            id,
            first_name,
            last_name,
            city,
            email,
            status,
            load_ts
        )
    """
    # --- Prepare raw data ---
    cursor.execute(insert_sql_1)

    logger.info("Insert batch 1 executed successfully.")

    run_dim_update(
        conn=conn,
        trino_catalog=TRINO_CATALOG,
        trino_schema=TRINO_SCHEMA,
        raw_table_name="employees",
        dim_table_name="dim_employees",
        scd2_view_name="view_employees_scd2",
        load_ts=load_ts_1,
        pk_col="id",
        cols_with_type=cols_with_type,
        current_timestamp=current_ts_1
    )

    rows = select_from_table("employees", order_by_cols=["load_ts", "id"])
    rows = select_from_table("dim_employees", order_by_cols=["id", "dp_valid_from"])

    expected = [
        (1, "Alice", "Meyer", "Zurich", "alice.meyer@example.com",
        f"{load_ts_1} 00:00:00", "9999-12-31 23:59:59", True, True,
        f"{load_ts_1} 00:00:00", current_ts_1, "9999-12-31 23:59:59",
        "NEW", "FF118EED04F8A2D0133E79435F7BC3CEBC0011D256A07FE02953CD12B3E29E51"),

        (2, "Bob", "Keller", "Bern", "bob.keller@example.com",
        f"{load_ts_1} 00:00:00", "9999-12-31 23:59:59", True, True,
        f"{load_ts_1} 00:00:00", current_ts_1, "9999-12-31 23:59:59",
        "NEW", "68844625A41E2D2540D4A17FBC7B51B3733C95FC58817DA05765F111F4F659CE"),

        (3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
        f"{load_ts_1} 00:00:00", "9999-12-31 23:59:59", True, True,
        f"{load_ts_1} 00:00:00", current_ts_1, "9999-12-31 23:59:59",
        "NEW", "67A87A1E14991AF623E8AC26518B9BB757E481E9B47AE9CBC728833FDDCEF86E"),
    ]

    # Convert DATE to string for stable comparison
    rows = [( *r[:-1], str(r[-1]) ) for r in rows]

    assert rows == expected

def test_step_2():
    cursor = conn.cursor()

    # --- Insert statement (batch 2) ---
    insert_sql_2 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.employees
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', DATE '{load_ts_2}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', DATE '{load_ts_2}')
        ) AS t (
            id,
            first_name,
            last_name,
            city,
            email,
            status,
            load_ts
        )
    """
    # --- Prepare raw data ---
    cursor.execute(insert_sql_2)

    logger.info("Insert batch 2 executed successfully.")

    run_dim_update(
        conn=conn,
        trino_catalog=TRINO_CATALOG,
        trino_schema=TRINO_SCHEMA,
        raw_table_name="employees",
        dim_table_name="dim_employees",
        scd2_view_name="view_employees_scd2",
        load_ts=load_ts_2,
        pk_col="id",
        cols_with_type=cols_with_type,
        current_timestamp=current_ts_2
    )
