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

FILE_NAME="reports/scd2_compnull_test_ins.md"

load_ts_1= datetime.strptime('2026-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_1 = datetime.strptime('2026-01-02 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_2 = datetime.strptime('2026-01-05 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_2 = datetime.strptime('2026-01-06 00:00:00', '%Y-%m-%d %H:%M:%S')

conn = init_trino_connection()

def test_step_1():
    logger.info("-------------------------------- Test Step 1 --------------------------------")

    create_raw_table(conn, pk_columns_with_type=["id1 INT, id2 INT"])
    create_dim_table_for_test(conn, pk_columns_with_type=["id1 INT, id2 INT"])
    render_init("Testing Insert Operation with a Composite Key (with NULL values)", FILE_NAME)
    render_data("This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities.", output_file_name=FILE_NAME)

    test_description = "Insert 3 entities into raw table and perform initial SCD2 merge."

    # --- Insert statement (batch 1) ---
    insert_sql_1 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (2, NULL, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (2, 3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
        ) AS t (
            id1,
            id2,
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
        (1, 1,"Alice", "Meyer", "Zurich", "alice.meyer@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "FC32620040E739795BE9C7EF23702C97E362C4C2BAAC8B6CAADE58A27DC1087A"),

        (2, 3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "B4A150F16BA0FBF1C18B07837F55DA9C16C18B4E699B8B92E6525DD6607F52C1"),

        (2, None, "Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40"),

    ]

    # run test
    scd2_merge_as_test(conn, test_step=1, ins_stmt=insert_sql_1, pk_columns=["id1", "id2"], load_ts=load_ts_1, current_ts=current_ts_1, expected=expected, output_file_name=FILE_NAME, test_description=test_description)

def test_step_2():
    logger.info("-------------------------------- Test Step 2 --------------------------------")

    cursor = conn.cursor()

    test_description = f"At {load_ts_2}, insert the new entity with `id=10` into the new partition of the raw table and perform SCD2 merge."

    # --- Insert statement (batch 2) ---
    insert_sql_2 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (2, NULL, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (2, 3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (10, 10, 'Kevin', 'Loosli', 'Bern', 'kevin.loosli@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}')
            ) AS t (
            id1,
            id2,
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
        (1, 1, "Alice", "Meyer", "Zurich", "alice.meyer@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "FC32620040E739795BE9C7EF23702C97E362C4C2BAAC8B6CAADE58A27DC1087A"),

        (2, 3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "B4A150F16BA0FBF1C18B07837F55DA9C16C18B4E699B8B92E6525DD6607F52C1"),

        (2, None,"Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40"),

        (10, 10, "Kevin", "Loosli", "Bern", "kevin.loosli@example.com",
        load_ts_2, MAX_TS, True, True,
        current_ts_2, MAX_TS,
        "DB6D2FC1F766B81756761381B965CE5D13E4AE3F8BF50E66BB2188214DC1B55C"),
    ]

    # run test
    scd2_merge_as_test(conn, test_step=2, ins_stmt=insert_sql_2, pk_columns=["id1", "id2"], load_ts=load_ts_2, current_ts=current_ts_2, expected=expected, output_file_name=FILE_NAME, test_description=test_description, perform_merge_op=True)


