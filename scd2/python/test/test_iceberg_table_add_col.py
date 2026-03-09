import sys
import os
import logging
import numpy as np

from datetime import date, timedelta, datetime
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_init, render_table, render_data, get_table_data, diff_with_color
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from constants import MAX_TS
from commons import TRINO_CATALOG, TRINO_SCHEMA, S3_WAREHOUSE_BUCKET, S3_WAREHOUSE_PREFIX, RAW_TABLE_NAME, COLS_WITH_TYPE, scd2_merge_as_test, create_raw_table, scd2_sel_as_test

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME="reports/test_iceberg_table_add_col.md"

load_ts_1= datetime.strptime('2026-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_1 = datetime.strptime('2026-01-02 00:00:00', '%Y-%m-%d %H:%M:%S')

def test_step_1(trino_conn):
    logger.info("-------------------------------- Test Step 1 --------------------------------")

    create_raw_table(trino_conn)

    render_init("Testing Add Column to existing Iceberg table", FILE_NAME)
    render_data("This test validates an ALTER TABLE ADD COLUMN operation on an existing Iceberg table.", output_file_name=FILE_NAME)

    render_data(f"## Test Step 1", output_file_name=FILE_NAME)

    # Prepare --- Insert statements ---
    insert_sql = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        VALUES
            (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
    """

    trino_conn.cursor().execute(insert_sql)

    df_before = get_table_data(trino_conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}", order_by_cols=[])
    render_table(df_before, title=f"Table {RAW_TABLE_NAME} before ADD COLUMN", output_file_name=FILE_NAME)

    rename_stmt = f"""
                    ALTER TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
                    ADD COLUMN new_col VARCHAR AFTER email
                    """
    print(rename_stmt)
    render_data(f"Executing ADD COLUMN", output_file_name=FILE_NAME)
    trino_conn.cursor().execute(rename_stmt)

    update_sql = f"""
        UPDATE {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SET new_col = 'New Value'
    """
    trino_conn.cursor().execute(update_sql)

    # Run SELECT test
    test_description = f"Select all the latest data. Even though Bob has been deleted it will still be shown because we are selecting the latest records as of today."

    # Run SELECT test
    sel_stmt = f"""
        SELECT * 
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        ORDER BY id
        """
    
    expected = [
        (1, "Alice", "Meyer", "Zurich", "alice.meyer@example.com", "New Value", "ACTIVE", load_ts_1, load_ts_1),
        (2, "Bob", "Keller", "Bern", "bob.keller@example.com", "New Value", "ACTIVE", load_ts_1, load_ts_1),
        (3, "Clara", "Schmid", "Basel", "clara.schmid@example.com", "New Value", "ACTIVE", load_ts_1, load_ts_1),
    ]

    scd2_sel_as_test(trino_conn, sel_stmt=sel_stmt, expected=expected, output_file_name=FILE_NAME, test_description=test_description)

