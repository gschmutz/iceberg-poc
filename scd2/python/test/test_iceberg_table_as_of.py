import sys
import os
import logging
import numpy as np

from datetime import date, timedelta, datetime
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_init, render_table, render_data, get_table_data, diff_with_color
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from scd2 import merge_into_dim_table, optimize_table
from constants import MAX_TS
from commons import TRINO_CATALOG, TRINO_SCHEMA, S3_WAREHOUSE_BUCKET, S3_WAREHOUSE_PREFIX, RAW_TABLE_NAME, COLS_WITH_TYPE, scd2_merge_as_test, create_raw_table, init_trino_connection, scd2_sel_as_test

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME="reports/test_iceberg_table_as_of.md"

load_ts_1= datetime.strptime('2026-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_1 = datetime.strptime('2026-01-02 00:00:00', '%Y-%m-%d %H:%M:%S')
conn = init_trino_connection()

def test_step_1():
    logger.info("-------------------------------- Test Step 1 --------------------------------")

    create_raw_table(conn)

    render_init("Testing Timetravel", FILE_NAME)
    render_data("This test validates an SELECT ... FOR VERSION AS OF operation on an existing Iceberg table.", output_file_name=FILE_NAME)

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

    # Prepare --- Update statement to add value to new_col ---
    update_sql = f"""
        UPDATE {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SET email = 'alice.meyer@newcorp.com'
        WHERE id = 1
    """
    conn.cursor().execute(update_sql)

    # Run SELECT test
    sel_stmt = f"""
        SELECT * 
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        ORDER BY id
        """
    
    expected = [
        (1, "Alice", "Meyer", "Zurich", "alice.meyer@newcorp.com", "ACTIVE", load_ts_1),
        (2, "Bob", "Keller", "Bern", "bob.keller@example.com", "ACTIVE", load_ts_1),
        (3, "Clara", "Schmid", "Basel", "clara.schmid@example.com", "ACTIVE", load_ts_1),
    ]

    # Run SELECT test
    test_description = f"Select all the latest data. Even though Bob has been deleted it will still be shown because we are selecting the latest records as of today."
    scd2_sel_as_test(conn, sel_stmt=sel_stmt, expected=expected, output_file_name=FILE_NAME, test_description=test_description)

def test_step_2():
    logger.info("-------------------------------- Test Step 2 --------------------------------")

    sel_snapshot_id = f'''
        SELECT snapshot_id 
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}."{RAW_TABLE_NAME}$snapshots" 
        WHERE parent_id IS NOT NULL 
        ORDER BY committed_at 
        LIMIT 1
        ''' 
    snapshot_id = conn.cursor().execute(sel_snapshot_id).fetchone()[0]

    # Run SELECT test
    sel_stmt = f"""
        SELECT * 
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        FOR VERSION AS OF {snapshot_id}
        ORDER BY id
        """
    
    expected = [
        (1, "Alice", "Meyer", "Zurich", "alice.meyer@example.com", "ACTIVE", load_ts_1),
        (2, "Bob", "Keller", "Bern", "bob.keller@example.com", "ACTIVE", load_ts_1),
        (3, "Clara", "Schmid", "Basel", "clara.schmid@example.com", "ACTIVE", load_ts_1),
    ]

    test_description = f"Select all the latest data. Even though Bob has been deleted it will still be shown because we are selecting the latest records as of today."
    scd2_sel_as_test(conn, sel_stmt=sel_stmt, expected=expected, output_file_name=FILE_NAME, test_description=test_description)

