import sys
import os
import logging
from datetime import date, timedelta, datetime
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_init, render_data
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from constants import MAX_TS
from commons import TRINO_CATALOG, TRINO_SCHEMA, S3_WAREHOUSE_BUCKET, S3_WAREHOUSE_PREFIX, DIM_TABLE_NAME, RAW_TABLE_NAME, SCD2_VIEW_NAME, EXCLUDE_COLS, COLS_WITH_TYPE, create_dim_table_for_test, scd2_merge_as_test, create_raw_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME="reports/scd2_test_upd_past_diff_val.md"

load_ts_1= datetime.strptime('2026-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_1 = datetime.strptime('2026-01-02 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_2 = datetime.strptime('2026-01-05 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_2 = datetime.strptime('2026-01-06 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_3 = datetime.strptime('2026-01-10 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_3 = datetime.strptime('2026-01-11 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_4 = datetime.strptime('2026-01-15 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_4 = datetime.strptime('2026-01-16 00:00:00', '%Y-%m-%d %H:%M:%S')

def test_step_1(trino_conn, spark):
    logger.info("-------------------------------- Test Step 1 --------------------------------")

    create_raw_table(trino_conn)
    create_dim_table_for_test(trino_conn, spark)
    
    render_init("Testing Update Operation with correction (different value) in the past", FILE_NAME)
    render_data("This test validates an UPDATE operation of one entity (with a new version with different value) on a set of existing entities.", output_file_name=FILE_NAME)

    test_description = "Insert 3 entities into raw table and perform initial SCD2 merge."

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
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676"),
    ]

    # run test
    scd2_merge_as_test(trino_conn, spark, test_step=1, ins_stmt=insert_sql_1, load_ts=load_ts_1, current_ts=current_ts_1, expected=expected, output_file_name=FILE_NAME, test_description=test_description)

def test_step_2(trino_conn, spark):
    logger.info("-------------------------------- Test Step 2 --------------------------------")

    test_description = f"At {load_ts_3}, update entity with `id=1` by setting city to bern and perform SCD2 merge."

    # --- Insert statement (batch 2) ---
    insert_sql_2 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Bern', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_3}', TIMESTAMP '{load_ts_3}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_3}', TIMESTAMP '{load_ts_3}'),
                (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_3}', TIMESTAMP '{load_ts_3}')
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
        load_ts_1, load_ts_3 - timedelta(seconds=1), False, False,
        current_ts_1, current_ts_3,
        "00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8"),

        (1, "Alice", "Meyer", "Bern", "alice.meyer@example.com",
        load_ts_3, MAX_TS, True, True,
        current_ts_3, MAX_TS,
        "6449C8A21EC1B7B2BD4891618CF5853B27A97968D41570EE3CD34617BDBBD7BD"),

        (2, "Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40"),

        (3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676"),
    ]

    # run test
    scd2_merge_as_test(trino_conn, spark, test_step=2, ins_stmt=insert_sql_2, load_ts=load_ts_3, current_ts=current_ts_3, expected=expected, output_file_name=FILE_NAME, test_description=test_description)


def test_step_3(trino_conn, spark):
    logger.info("-------------------------------- Test Step 3 --------------------------------")

    test_description = f"At {load_ts_2}, update entity with `id=1` in raw table by setting city to basel and perform SCD2 merge."

    # --- Insert statement (batch 2) ---
    insert_sql = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Basel', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}')
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
        load_ts_1, load_ts_2 - timedelta(seconds=1), False, False,
        current_ts_1, current_ts_4,
        "00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8"),

        (1, "Alice", "Meyer", "Basel", "alice.meyer@example.com",
        load_ts_2, load_ts_3 - timedelta(seconds=1), False, False,
        current_ts_4, MAX_TS,
        "82A7F0902502004847DA4EF33BA6F56F3D0C1FF5D1A4766CB53EC817CAEBBAA0"),

        (1, "Alice", "Meyer", "Bern", "alice.meyer@example.com",
        load_ts_3, MAX_TS, True, True,
        current_ts_3, MAX_TS,
        "6449C8A21EC1B7B2BD4891618CF5853B27A97968D41570EE3CD34617BDBBD7BD"),

        (2, "Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40"),

        (3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676"),
    ]

    # run test
    scd2_merge_as_test(trino_conn, spark, test_step=3, ins_stmt=insert_sql, load_ts=load_ts_2, current_ts=current_ts_4, expected=expected, output_file_name=FILE_NAME, test_description=test_description, perform_merge_op=True)

