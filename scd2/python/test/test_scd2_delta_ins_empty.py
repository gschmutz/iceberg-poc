import sys
import os
import logging
from datetime import date, timedelta, datetime
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_init, render_data
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from scd2 import merge_into_dim_table
from constants import MAX_TS
from commons import TRINO_CATALOG, TRINO_SCHEMA, S3_WAREHOUSE_BUCKET, S3_WAREHOUSE_PREFIX, DIM_TABLE_NAME, RAW_TABLE_NAME, SCD2_VIEW_NAME, EXCLUDE_COLS, COLS_WITH_TYPE, create_dim_table_for_test, scd2_merge_as_test, create_raw_table, init_trino_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME="reports/scd2_delta_test_ins_empty.md"

load_ts_1= datetime.strptime('2026-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_1 = datetime.strptime('2026-01-02 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_2 = datetime.strptime('2026-01-05 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_2 = datetime.strptime('2026-01-06 00:00:00', '%Y-%m-%d %H:%M:%S')

conn = init_trino_connection()

def test_step_1():
    logger.info("-------------------------------- Test Step 1 --------------------------------")

    create_raw_table(conn)
    create_dim_table_for_test(conn)
    render_init("Testing Insert Operation (Delta Mode for Source)", FILE_NAME)
    render_data("This test validates an INSERT operation of one single entity into an empty dimension table.", output_file_name=FILE_NAME)

    test_description = f"At {load_ts_1}, insert 1 entity into raw table and perform initial SCD2 merge."

    # --- Insert statement (batch 1) ---
    insert_sql_1 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
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

    expected = [
        (1, "Alice", "Meyer", "Zurich", "alice.meyer@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, current_ts_1, MAX_TS,
        "NEW", "FF118EED04F8A2D0133E79435F7BC3CEBC0011D256A07FE02953CD12B3E29E51"),
    ]

    # run test
    scd2_merge_as_test(conn, test_step=1, ins_stmt=insert_sql_1, load_ts=load_ts_1, current_ts=current_ts_1, expected=expected, output_file_name=FILE_NAME, test_description=test_description, use_delta_mode_for_raw_table=True, perform_merge_op=True)

