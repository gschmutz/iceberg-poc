import sys
import os
import logging
from datetime import date, timedelta, datetime
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_init, render_data
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from constants import MAX_TS
from commons import TRINO_CATALOG, TRINO_SCHEMA, S3_WAREHOUSE_BUCKET, S3_WAREHOUSE_PREFIX, DIM_TABLE_NAME, RAW_TABLE_NAME, SCD2_VIEW_NAME, EXCLUDE_COLS, COLS_WITH_TYPE, create_dim_table_for_test, scd2_merge_as_test,scd2_merge_as_preparation, create_raw_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME="reports/scd2_test_fill_gap_partial_same_val_next.md"

load_ts_1= datetime.strptime('2026-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_1 = datetime.strptime('2026-01-02 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_2 = datetime.strptime('2026-01-05 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_2 = datetime.strptime('2026-01-06 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_3 = datetime.strptime('2026-01-10 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_3 = datetime.strptime('2026-01-11 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_4 = datetime.strptime('2026-01-15 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_4 = datetime.strptime('2026-01-16 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_5 = datetime.strptime('2026-01-20 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_5 = datetime.strptime('2026-01-21 00:00:00', '%Y-%m-%d %H:%M:%S')

def test_step_1(trino_conn, spark):
    logger.info("-------------------------------- Test Step 1 --------------------------------")

    create_raw_table(trino_conn)
    create_dim_table_for_test(trino_conn, spark)
    render_init("Testing Filling a gap with the same value as the next version after the gap", FILE_NAME)
    render_data("This test validates filling the gap in a single entity. The record added into the gap is having the same values as the version following the gap.", output_file_name=FILE_NAME)

    test_description = f"At {load_ts_1}, insert 3 records, at {load_ts_2} delete the one with id=3 and reinsert id=3 at {load_ts_4} into raw table and perform initial SCD2 merge."

    # --- Insert statement (batch 1) ---
    insert_sql_1 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
        ) AS t (
            id,
            first_name,
            last_name,
            city,
            email,
            status,
            dp_ts_from,
            dp_loaded_at
        )
    """

    # --- Insert statement (batch 2) ---
    insert_sql_2 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'INACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}')
        ) AS t (
            id,
            first_name,
            last_name,
            city,
            email,
            status,
            dp_ts_from,
            dp_loaded_at
        )
    """

    # --- Insert statement (batch 3) ---
    insert_sql_3 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_4}', TIMESTAMP '{load_ts_4}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_4}', TIMESTAMP '{load_ts_4}'),
                (3, 'Clara', 'Schmid', 'Geneva', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_4}', TIMESTAMP '{load_ts_4}')
        ) AS t (
            id,
            first_name,
            last_name,
            city,
            email,
            status,
            dp_ts_from,
            dp_loaded_at
        )
    """
    expected = [
        (1, "Alice", "Meyer", "Zurich", "alice.meyer@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8"),

        (2, "Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40"),

        (3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
        load_ts_1, load_ts_2 - timedelta(seconds=1), False, False,
        current_ts_1, current_ts_4,
        "77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676"),

        (3, "Clara", "Schmid", "Geneva", "clara.schmid@example.com",
        load_ts_4, MAX_TS, True, True,
        current_ts_4, MAX_TS,
        "777BB26D490500D4BF4E829691C85C2DF112D21B4D205D879812E5BE99529853"),

    ]

    scd2_merge_as_preparation(trino_conn, spark, 
                              ins_stmts=[insert_sql_1, insert_sql_2, insert_sql_3], 
                              load_ts_list=[load_ts_1, load_ts_2, load_ts_4], 
                              current_ts_list=[current_ts_1, current_ts_2, current_ts_4], expected=expected, output_file_name=FILE_NAME, test_description=test_description)


def test_step_2(trino_conn, spark):
    logger.info("-------------------------------- Test Step 2 --------------------------------")

    test_description = f"Fill the gap partially at {load_ts_3} by adding a record with the same values as the version following the gap."

    # --- Insert statement (batch 2) ---
    insert_sql_1 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_4}', TIMESTAMP '{load_ts_5}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_4}', TIMESTAMP '{load_ts_5}'),
                (3, 'Clara', 'Schmid', 'Geneva', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_3}', TIMESTAMP '{load_ts_5}')
            ) AS t (
            id,
            first_name,
            last_name,
            city,
            email,
            status,
            dp_ts_from,
            dp_loaded_at
        )
    """

    expected = [
        (1, "Alice", "Meyer", "Zurich", "alice.meyer@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8"),

        (2, "Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40"),

        (3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
        load_ts_1, load_ts_2 - timedelta(seconds=1), False, False,
        current_ts_1, current_ts_4,
        "77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676"),

        (3, "Clara", "Schmid", "Geneva", "clara.schmid@example.com",
        load_ts_3, MAX_TS, True, True,
        current_ts_4, current_ts_5,
        "777BB26D490500D4BF4E829691C85C2DF112D21B4D205D879812E5BE99529853"),

    ]

    # run test
    scd2_merge_as_test(trino_conn, spark, test_step=2, ins_stmt=insert_sql_1, load_ts=load_ts_5, current_ts=current_ts_5, expected=expected, output_file_name=FILE_NAME, test_description=test_description, 
                        perform_merge_op=True, use_delta_mode_for_raw_table=False)

