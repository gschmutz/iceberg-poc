import sys
import os
import logging
import numpy as np

from datetime import date, timedelta, datetime
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_init, render_table, render_data, get_table_data, diff_with_color
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from scd2 import merge_into_dim_table, create_dim_table, optimize_table
from constants import MAX_TS
from commons import TRINO_CATALOG, TRINO_SCHEMA, S3_WAREHOUSE_BUCKET, S3_WAREHOUSE_PREFIX, RAW_TABLE_NAME, COLS_WITH_TYPE, scd2_merge_as_test, create_raw_table, init_trino_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME="reports/test_iceberg_table_add_col.md"

load_ts_1= datetime.strptime('2026-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_1 = datetime.strptime('2026-01-02 00:00:00', '%Y-%m-%d %H:%M:%S')

conn = init_trino_connection()

#@pytest.fixture(autouse=True, scope="session")
#def setup_data(request):
#    create_raw_table()
#    create_dim_table(conn, TRINO_CATALOG, TRINO_SCHEMA, "{DIM_TABLE_NAME}", s3_warehouse_bucket=S3_WAREHOUSE_BUCKET, s3_warehouse_prefix=S3_WAREHOUSE_PREFIX, pk_col_with_type="id INT", cols_with_type=cols_with_type, partition_cols=["dp_valid_from"], sort_cols=[])
#    yield
#    logger.info("Finished all tests")


def test_step_1():
    logger.info("-------------------------------- Test Step 1 --------------------------------")

    create_raw_table(conn)

    render_init("Testing Insert Operation", FILE_NAME)
    render_data("This test validates an INSERT operation of one new record", output_file_name=FILE_NAME)

    render_data(f"## Test Step 1", output_file_name=FILE_NAME)

    # Prepare --- Insert statements ---
    insert_sql = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        VALUES
            (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}'),
            (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}'),
            (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}')
    """

    conn.cursor().execute(insert_sql)

    df_before = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}", order_by_cols=[])
    render_table(df_before, title=f"### Table {RAW_TABLE_NAME} before ADD COLUMN", output_file_name=FILE_NAME)

    rename_stmt = f"""
                    ALTER TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
                    ADD COLUMN new_col VARCHAR AFTER email
                    """
    print(rename_stmt)
    render_data(f"Executing ADD COLUMN", output_file_name=FILE_NAME)
    conn.cursor().execute(rename_stmt)

    df_after = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}")
    render_table(df_after, title=f"### Table {RAW_TABLE_NAME} after ADD COLUMN", output_file_name=FILE_NAME)

def test_step_2():
    logger.info("-------------------------------- Test Step 2 --------------------------------")

    render_data(f"## Test Step 2", output_file_name=FILE_NAME)

    # Prepare --- Update statement to add value to new_col ---
    update_sql = f"""
        UPDATE {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SET new_col = 'New Value'
    """

    conn.cursor().execute(update_sql)

    df_before = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}", order_by_cols=[])
    render_table(df_before, title=f"### Table {RAW_TABLE_NAME}", output_file_name=FILE_NAME)

def test_step_3():
    logger.info("-------------------------------- Test Step 3 --------------------------------")

    render_data(f"## Test Step 3", output_file_name=FILE_NAME)

    sel_snapshot_id = f'''
        SELECT snapshot_id 
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}."{RAW_TABLE_NAME}$snapshots" 
        WHERE parent_id IS NOT NULL 
        ORDER BY committed_at 
        LIMIT 1
        ''' 
    snapshot_id = conn.cursor().execute(sel_snapshot_id).fetchone()[0]

    df_before = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}", order_by_cols=[], for_version=snapshot_id)
    render_table(df_before, title=f"### Table {RAW_TABLE_NAME}", output_file_name=FILE_NAME)


