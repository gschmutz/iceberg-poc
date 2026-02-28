import sys
import os
import logging
from datetime import date, timedelta, datetime
import logging
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from util import get_param, get_credential, replace_vars_in_string, render_init, render_data, get_table_data, render_table
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from scd2 import merge_into_dim_table
from constants import MAX_TS
from commons import TRINO_CATALOG, TRINO_SCHEMA, S3_WAREHOUSE_BUCKET, S3_WAREHOUSE_PREFIX, DIM_TABLE_NAME, RAW_TABLE_NAME, SCD2_VIEW_NAME, EXCLUDE_COLS, COLS_WITH_TYPE, create_dim_table_for_test, scd2_merge_as_test, scd2_sel_as_test, scd2_merge_as_preparation, create_raw_table, init_trino_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME="reports/scd2_test_sel_is_active.md"

load_ts_1= datetime.strptime('2026-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_1 = datetime.strptime('2026-01-02 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_2 = datetime.strptime('2026-01-05 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_2 = datetime.strptime('2026-01-06 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_3 = datetime.strptime('2026-01-10 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_3 = datetime.strptime('2026-01-11 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_4 = datetime.strptime('2026-01-20 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_4 = datetime.strptime('2026-01-21 00:00:00', '%Y-%m-%d %H:%M:%S')

conn = init_trino_connection()

def test_step_1():
    logger.info("-------------------------------- Test Step 1 --------------------------------")

    create_raw_table(conn)
    create_dim_table_for_test(conn)
    
    render_init("Testing for valid data at a given at a given timestamp", FILE_NAME)
    render_data(f"This test validates a single SELECT operation for data valid at a timestamp {load_ts_2 - timedelta(days=2)}", output_file_name=FILE_NAME)

    # --- Insert statement (batch 1) ---
    insert_sql_1 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
        ) AS t (
            id,
            first_name,
            last_name,
            city,
            email,
            status,
            dp_valid_from,
            dp_loaded_at
        )
    """
    # --- Insert statement (batch 2) ---
    insert_sql_2 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Bern', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'INACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}')
            ) AS t (
            id,
            first_name,
            last_name,
            city,
            email,
            status,
            dp_valid_from,
            dp_loaded_at
        )
    """
    scd2_merge_as_preparation(conn, ins_stmts=[insert_sql_1,insert_sql_2]
                              , load_ts_list=[load_ts_1, load_ts_2], current_ts_list=[current_ts_1, current_ts_2]
                              , output_file_name=FILE_NAME)

    # Run SELECT test
    test_description = f"Select all the active data. Because Bob has been deleted at {load_ts_2} it will no longer be shown when selecting only ACTIVE records as of today."

    # Run SELECT test
    sel_stmt = f"""
        SELECT id, first_name, last_name, city, email,
                dp_valid_from, dp_valid_to, dp_is_active, dp_is_latest,
                dp_load_timestamp, dp_created_at, dp_replaced_at,
                record_hash 
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{DIM_TABLE_NAME}
        WHERE dp_is_active = TRUE
        ORDER BY id
        """
    
    expected = [
        (1, "Alice", "Meyer", "Bern", "alice.meyer@example.com",
        load_ts_2, MAX_TS, True, True,
        current_ts_2, current_ts_2, MAX_TS,
        "6449C8A21EC1B7B2BD4891618CF5853B27A97968D41570EE3CD34617BDBBD7BD"),

        (3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
        load_ts_2, MAX_TS, True, True,
        current_ts_2, current_ts_2, MAX_TS,
        "77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676")
    ]

    scd2_sel_as_test(conn, sel_stmt=sel_stmt, expected=expected, output_file_name=FILE_NAME, test_description=test_description)


