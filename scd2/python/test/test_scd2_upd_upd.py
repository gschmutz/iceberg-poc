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

FILE_NAME="reports/scd2_test_upd_upd.md"

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
    
    render_init("Testing Multiple Update Operations on same entity but different fields", FILE_NAME)
    render_data("This test validates multiple UPDATE operations on one entity over time producing many versions.", output_file_name=FILE_NAME)

    test_description = "Insert 2 entities into raw table and perform initial SCD2 merge."

    # --- Insert statement (batch 1) ---
    insert_sql_1 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}')
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
    ]

    # run test
    scd2_merge_as_test(conn, test_step=1, ins_stmt=insert_sql_1, load_ts=load_ts_1, current_ts=current_ts_1, expected=expected, output_file_name=FILE_NAME, test_description=test_description)

def test_step_2():
    logger.info("-------------------------------- Test Step 2 --------------------------------")

    cursor = conn.cursor()

    test_description = f"At {load_ts_2}, update `city` of entity with `id=1` and perform SCD2 merge."

    # --- Insert statement (batch 2) ---
    insert_sql_2 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Bern', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}'),
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
        load_ts_1, load_ts_2 - timedelta(seconds=1), False, False,
        current_ts_1, current_ts_1, current_ts_2,
        "SUPERSEDED", "FF118EED04F8A2D0133E79435F7BC3CEBC0011D256A07FE02953CD12B3E29E51"),

        (1, "Alice", "Meyer", "Bern", "alice.meyer@example.com",
        load_ts_2, MAX_TS, True, True,
        current_ts_2, current_ts_2, MAX_TS,
        "SUPERSEDED_BY", "67B1EB7F635FBBC16C2FFA0EAD786E929C4D1F8E26B210ABFE37D0CFB73EDE39"),

        (2, "Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, current_ts_1, MAX_TS,
        "NEW", "68844625A41E2D2540D4A17FBC7B51B3733C95FC58817DA05765F111F4F659CE")
    ]

    # run test
    scd2_merge_as_test(conn, test_step=2, ins_stmt=insert_sql_2, load_ts=load_ts_2, current_ts=current_ts_2, expected=expected, output_file_name=FILE_NAME, test_description=test_description)

def test_step_3():
    logger.info("-------------------------------- Test Step 3 --------------------------------")

    cursor = conn.cursor()

    test_description = f"At {load_ts_3}, update `email` of entity with `id=1` and perform SCD2 merge."

    # --- Insert statement (batch 3) ---
    insert_sql_3 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Bern', 'alice.meyer@newmail.com', 'ACTIVE', TIMESTAMP '{load_ts_3}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_3}')
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
        load_ts_1, load_ts_2 - timedelta(seconds=1), False, False,
        current_ts_1, current_ts_1, current_ts_2,
        "SUPERSEDED", "FF118EED04F8A2D0133E79435F7BC3CEBC0011D256A07FE02953CD12B3E29E51"),

        (1, "Alice", "Meyer", "Bern", "alice.meyer@example.com",
        load_ts_2, load_ts_3 - timedelta(seconds=1), False, False,
        current_ts_2, current_ts_2, current_ts_3,
        "SUPERSEDED", "67B1EB7F635FBBC16C2FFA0EAD786E929C4D1F8E26B210ABFE37D0CFB73EDE39"),

        (1, "Alice", "Meyer", "Bern", "alice.meyer@newmail.com",
        load_ts_3, MAX_TS, True, True,
        current_ts_3, current_ts_3, MAX_TS,
        "SUPERSEDED_BY", "062C5F0E27D916E1E63C4ED339518C42D26BF8A32A5BA01F12B2524C03E1E2FB"),

        (2, "Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, current_ts_1, MAX_TS,
        "NEW", "68844625A41E2D2540D4A17FBC7B51B3733C95FC58817DA05765F111F4F659CE")
    ]

    # run test
    scd2_merge_as_test(conn, test_step=3, ins_stmt=insert_sql_3, load_ts=load_ts_3, current_ts=current_ts_3, expected=expected, output_file_name=FILE_NAME, test_description=test_description, perform_merge_op=True)


def test_step_4():
    logger.info("-------------------------------- Test Step 4 --------------------------------")

    cursor = conn.cursor()

    test_description = f"At {load_ts_4}, update `last_name` of entity with `id=1` and perform SCD2 merge."

    # --- Insert statement (batch 4) ---
    insert_sql_4 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Müller-Meyer', 'Bern', 'alice.meyer@newmail.com', 'ACTIVE', TIMESTAMP '{load_ts_4}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_4}')
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
        load_ts_1, load_ts_2 - timedelta(seconds=1), False, False,
        current_ts_1, current_ts_1, current_ts_2,
        "SUPERSEDED", "FF118EED04F8A2D0133E79435F7BC3CEBC0011D256A07FE02953CD12B3E29E51"),

        (1, "Alice", "Meyer", "Bern", "alice.meyer@example.com",
        load_ts_2, load_ts_3 - timedelta(seconds=1), False, False,
        current_ts_2, current_ts_2, current_ts_3,
        "SUPERSEDED", "67B1EB7F635FBBC16C2FFA0EAD786E929C4D1F8E26B210ABFE37D0CFB73EDE39"),

        (1, "Alice", "Meyer", "Bern", "alice.meyer@newmail.com",
        load_ts_3, load_ts_4 - timedelta(seconds=1), False, False,
        current_ts_3, current_ts_3, current_ts_4,
        "SUPERSEDED", "062C5F0E27D916E1E63C4ED339518C42D26BF8A32A5BA01F12B2524C03E1E2FB"),

        (1, "Alice", "Müller-Meyer", "Bern", "alice.meyer@newmail.com",
        load_ts_4, MAX_TS, True, True,
        current_ts_4, current_ts_4, MAX_TS,
        "SUPERSEDED_BY", "950A75C7A8739691A38825C641F92F9C43F04D4F02E99A7A6EDF8FE689381A53"),

        (2, "Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, current_ts_1, MAX_TS,
        "NEW", "68844625A41E2D2540D4A17FBC7B51B3733C95FC58817DA05765F111F4F659CE")
    ]

    # run test
    scd2_merge_as_test(conn, test_step=4, ins_stmt=insert_sql_4, load_ts=load_ts_4, current_ts=current_ts_4, expected=expected, output_file_name=FILE_NAME, test_description=test_description, perform_merge_op=True)

