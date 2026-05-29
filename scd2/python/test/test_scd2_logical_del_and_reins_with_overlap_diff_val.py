import logging
import os
import sys
from datetime import date, datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../lib")))
from util import (
    render_data,
    render_init,
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../lib")))
from commons import (
    DELTA_MODE_DELETE_EXPRESSION,
    create_scd2_table_for_test,
    create_raw_table,
    get_strategy_name,
    source_table_fqn,
    scd2_merge_as_preparation,
    scd2_merge_as_test,
)
from constants import MAX_TS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME = f"reports/{get_strategy_name().lower()}/scd2_test_logical_del_and_reins_with_overlap_diff_val.md"

load_ts_1 = datetime.strptime("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_1 = datetime.strptime("2026-01-02 00:00:00", "%Y-%m-%d %H:%M:%S")

load_ts_2 = datetime.strptime("2026-01-05 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_2 = datetime.strptime("2026-01-06 00:00:00", "%Y-%m-%d %H:%M:%S")

load_ts_3 = datetime.strptime("2026-01-10 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_3 = datetime.strptime("2026-01-11 00:00:00", "%Y-%m-%d %H:%M:%S")

load_ts_4 = datetime.strptime("2026-01-15 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_4 = datetime.strptime("2026-01-16 00:00:00", "%Y-%m-%d %H:%M:%S")


def test_step_1(ctx):
    logger.info(
        "-------------------------------- Test Step 1 --------------------------------"
    )

    create_raw_table(ctx)
    create_scd2_table_for_test(ctx)
    render_init("Testing Reactivating a logically deleted record", FILE_NAME)
    render_data(
        "This test validates a REACTIVATE operation of a single entity. The reactivate is created by re-inserting the record in the raw table with an overlap of the deleted version.",
        output_file_name=FILE_NAME,
    )
    render_data("\n", output_file_name=FILE_NAME)
    render_data(f" * **Strategy:** `{get_strategy_name().lower()}`", output_file_name=FILE_NAME)
    render_data(f" * **Last Run:** `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`", output_file_name=FILE_NAME)

    test_description = "Insert 3 records into raw table and perform initial SCD2 merge."

    # --- Insert statement (batch 1) ---
    insert_sql_1 = f"""
        INSERT INTO {source_table_fqn(ctx)}
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
        (
            1,
            "Alice",
            "Meyer",
            "Zurich",
            "alice.meyer@example.com",
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "F244BC679F09400F6966D6472E66C079A59943FEC38344C4306149D8034ED570",
        ),
        (
            2,
            "Bob",
            "Keller",
            "Bern",
            "bob.keller@example.com",
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "E5181C5926D5185F99D4654A7F15E2476045F4808F22C5924E128B87DEB6F93F",
        ),
        (
            3,
            "Clara",
            "Schmid",
            "Basel",
            "clara.schmid@example.com",
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "254677EA92F6E7E5A9C2629DE097CA5B2821DC6CF93B283D7158EC37920083CB",
        ),
    ]

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=1,
        ins_stmt=insert_sql_1,
        dp_ts=load_ts_1,
        current_ts=current_ts_1,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
        use_logical_delete_for_source_table=True,
        logical_delete_expression=DELTA_MODE_DELETE_EXPRESSION
    )


def test_step_2(ctx):
    logger.info(
        "-------------------------------- Test Step 2 --------------------------------"
    )

    test_description = f"At {load_ts_3}, logically delete record with `id=3` from raw table and perform SCD2 merge."

    # --- Insert statement (batch 2) ---
    insert_sql_2 = f"""
        INSERT INTO {source_table_fqn(ctx)}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_3}', TIMESTAMP '{load_ts_3}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_3}', TIMESTAMP '{load_ts_3}'),
                (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'INACTIVE', TIMESTAMP '{load_ts_3}', TIMESTAMP '{load_ts_3}')
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
        (
            1,
            "Alice",
            "Meyer",
            "Zurich",
            "alice.meyer@example.com",
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "F244BC679F09400F6966D6472E66C079A59943FEC38344C4306149D8034ED570",
        ),
        (
            2,
            "Bob",
            "Keller",
            "Bern",
            "bob.keller@example.com",
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "E5181C5926D5185F99D4654A7F15E2476045F4808F22C5924E128B87DEB6F93F",
        ),
        (
            3,
            "Clara",
            "Schmid",
            "Basel",
            "clara.schmid@example.com",
            load_ts_1,
            load_ts_3 - timedelta(seconds=1),
            False,
            True,
            current_ts_1,
            current_ts_3,
            "254677EA92F6E7E5A9C2629DE097CA5B2821DC6CF93B283D7158EC37920083CB",
        ),
    ]

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=2,
        ins_stmt=insert_sql_2,
        dp_ts=load_ts_3,
        current_ts=current_ts_3,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
        use_logical_delete_for_source_table=True,
        logical_delete_expression=DELTA_MODE_DELETE_EXPRESSION
    )


def test_step_3(ctx):
    logger.info(
        "-------------------------------- Test Step 3 --------------------------------"
    )

    test_description = "Reactivate record with `id=3` by inserting it again with an overlap to the deleted version and with a different value for `city` than the deleted version and perform SCD2 merge."

    # --- Insert statement (batch 3) ---
    insert_sql_3 = f"""
        INSERT INTO {source_table_fqn(ctx)}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_4}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_4}'),
                (3, 'Clara', 'Schmid', 'Geneva', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_4}')
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
        (
            1,
            "Alice",
            "Meyer",
            "Zurich",
            "alice.meyer@example.com",
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "F244BC679F09400F6966D6472E66C079A59943FEC38344C4306149D8034ED570",
        ),
        (
            2,
            "Bob",
            "Keller",
            "Bern",
            "bob.keller@example.com",
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "E5181C5926D5185F99D4654A7F15E2476045F4808F22C5924E128B87DEB6F93F",
        ),
        (
            3,
            "Clara",
            "Schmid",
            "Basel",
            "clara.schmid@example.com",
            load_ts_1,
            load_ts_2 - timedelta(seconds=1),
            False,
            False,
            current_ts_1,
            current_ts_4,
            "254677EA92F6E7E5A9C2629DE097CA5B2821DC6CF93B283D7158EC37920083CB",
        ),
        (
            3,
            "Clara",
            "Schmid",
            "Geneva",
            "clara.schmid@example.com",
            load_ts_2,
            MAX_TS,
            True,
            True,
            current_ts_4,
            MAX_TS,
            "CA14C81ED1ABCD039C850A47BBE3ED8F509541787BF19B1C3D2BFEFD05D34C92",
        ),
    ]

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=3,
        ins_stmt=insert_sql_3,
        dp_ts=load_ts_4,
        current_ts=current_ts_4,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
        perform_merge_op=True,
        use_logical_delete_for_source_table=True,
        logical_delete_expression=DELTA_MODE_DELETE_EXPRESSION
    )
