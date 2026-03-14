import sys
import os
import logging
from datetime import date, timedelta, datetime
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_init, render_data
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from constants import MAX_TS
from commons import raw_table_fqn, S3_WAREHOUSE_BUCKET, S3_WAREHOUSE_PREFIX, RAW_TABLE_NAME, SCD2_VIEW_NAME, EXCLUDE_COLS, COLS_WITH_TYPE, create_dim_table_for_test, scd2_merge_as_test, create_raw_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME="reports/scd2_test_compkey_upd.md"

load_ts_1= datetime.strptime('2026-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_1 = datetime.strptime('2026-01-02 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_2 = datetime.strptime('2026-01-05 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_2 = datetime.strptime('2026-01-06 00:00:00', '%Y-%m-%d %H:%M:%S')

def test_step_1(ctx):
    logger.info("-------------------------------- Test Step 1 --------------------------------")
    
    create_raw_table(ctx, pk_columns_with_type=["id1 INT, id2 INT"])
    create_dim_table_for_test(ctx, pk_columns_with_type=["id1 INT, id2 INT"])
    
    render_init("Testing Update Operation with Composite Key", FILE_NAME)
    render_data("This test validates an UPDATE operation of one entity (with a new version) on a set of existing entities.", output_file_name=FILE_NAME)

    test_description = f"At {load_ts_1}, insert 3 entities into raw table and perform initial SCD2 merge."

    # --- Insert statement (batch 1) ---
    insert_sql_1 = f"""
        INSERT INTO {raw_table_fqn(ctx)}
        SELECT *
        FROM (
            VALUES
                (1, 1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (2, 2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (3, 3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
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

        (2, 2, "Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "BF95C839ED40F6745B2FFB0B3988C93FC14D92CD490A0BB26013F7A1F4748986"),

        (3, 3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "EFA3962E9F15A846EB1999A38C6B310F71E88BEDC22CEE2174B9C2B8A121524E"),
    ]

    # run test
    scd2_merge_as_test(ctx, test_step=1, ins_stmt=insert_sql_1, pk_columns=["id1", "id2"], load_ts=load_ts_1, current_ts=current_ts_1, expected=expected, output_file_name=FILE_NAME, test_description=test_description)

def test_step_2(ctx):
    logger.info("-------------------------------- Test Step 2 --------------------------------")

    test_description = f"At {load_ts_2}, update `email` of entity with `id=3` in raw table and perform SCD2 merge."

    # --- Insert statement (batch 2) ---
    insert_sql_2 = f"""
        INSERT INTO {raw_table_fqn(ctx)}
        SELECT *
        FROM (
            VALUES
                (1, 1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (2, 2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (3, 3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@newmail.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}')
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

        (2, 2, "Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, MAX_TS,
        "BF95C839ED40F6745B2FFB0B3988C93FC14D92CD490A0BB26013F7A1F4748986"),

        (3, 3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
        load_ts_1, load_ts_2 - timedelta(seconds=1), False, False,
        current_ts_1, current_ts_2,
        "EFA3962E9F15A846EB1999A38C6B310F71E88BEDC22CEE2174B9C2B8A121524E"),

        (3, 3, "Clara", "Schmid", "Basel", "clara.schmid@newmail.com",
        load_ts_2, MAX_TS, True, True,
        current_ts_2, MAX_TS,
        "8C01D872535978047B58D1C33D3CE3731B4E1A08F5E6F10D9659FB72C94807B1"),
    ]

    # run test
    scd2_merge_as_test(ctx, test_step=2, ins_stmt=insert_sql_2, pk_columns=["id1", "id2"], load_ts=load_ts_2, current_ts=current_ts_2, expected=expected, output_file_name=FILE_NAME, test_description=test_description)


