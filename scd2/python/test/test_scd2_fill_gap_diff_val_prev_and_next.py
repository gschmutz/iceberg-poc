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

FILE_NAME = f"reports/{get_strategy_name().lower()}/scd2_test_fill_gap_diff_val_prev_and_next.md"

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
    render_init(
        "Testing Filling a gap with different value than the next version after the gap",
        FILE_NAME,
    )
    render_data(
        "This test validates filling the gap in a single entity. The record added into the gap is having different values than the version following the gap.",
        output_file_name=FILE_NAME,
    )
    render_data("\n", output_file_name=FILE_NAME)
    render_data(f" * **Strategy:** `{get_strategy_name().lower()}`", output_file_name=FILE_NAME)
    render_data(f" * **Last Run:** `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`", output_file_name=FILE_NAME)

    test_description = f"At {load_ts_1}, insert 3 records, at {load_ts_2} delete the one with id=3 and reinsert id=3 at {load_ts_3} into raw table and perform initial SCD2 merge."

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

    # --- Insert statement (batch 2) ---
    insert_sql_2 = f"""
        INSERT INTO {source_table_fqn(ctx)}
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
        INSERT INTO {source_table_fqn(ctx)}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_3}', TIMESTAMP '{load_ts_3}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_3}', TIMESTAMP '{load_ts_3}'),
                (3, 'Clara', 'Schmid', 'Geneva', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_3}', TIMESTAMP '{load_ts_3}')
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
            current_ts_3,
            "254677EA92F6E7E5A9C2629DE097CA5B2821DC6CF93B283D7158EC37920083CB",
        ),
        (
            3,
            "Clara",
            "Schmid",
            "Geneva",
            "clara.schmid@example.com",
            load_ts_3,
            MAX_TS,
            True,
            True,
            current_ts_3,
            MAX_TS,
            "CA14C81ED1ABCD039C850A47BBE3ED8F509541787BF19B1C3D2BFEFD05D34C92",
        ),
    ]

    scd2_merge_as_preparation(
        ctx,
        ins_stmts=[insert_sql_1, insert_sql_2, insert_sql_3],
        dp_ts_list=[load_ts_1, load_ts_2, load_ts_3],
        current_ts_list=[current_ts_1, current_ts_2, current_ts_3],
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
        perform_merge_op=True,
        use_logical_delete_for_source_table=True,
        logical_delete_expression=DELTA_MODE_DELETE_EXPRESSION
    )


def test_step_2(ctx):
    logger.info(
        "-------------------------------- Test Step 2 --------------------------------"
    )

    test_description = f"Fill the gap at {load_ts_2} by adding a record with different values as the version preceding and following the gap."

    # --- Insert statement (batch 2) ---
    insert_sql_1 = f"""
        INSERT INTO {source_table_fqn(ctx)}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_4}', TIMESTAMP '{load_ts_4}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_4}', TIMESTAMP '{load_ts_4}'),
                (3, 'Clara', 'Schmid', 'Zurich', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_4}')
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
            current_ts_3,
            "254677EA92F6E7E5A9C2629DE097CA5B2821DC6CF93B283D7158EC37920083CB",
        ),
        (
            3,
            "Clara",
            "Schmid",
            "Zurich",
            "clara.schmid@example.com",
            load_ts_2,
            load_ts_3 - timedelta(seconds=1),
            False,
            False,
            current_ts_4,
            MAX_TS,
            "97208ADCC2585C00A2027A69920677917C10C993F82DB817D48C0C2F10541382",
        ),
        (
            3,
            "Clara",
            "Schmid",
            "Geneva",
            "clara.schmid@example.com",
            load_ts_3,
            MAX_TS,
            True,
            True,
            current_ts_3,
            MAX_TS,
            "CA14C81ED1ABCD039C850A47BBE3ED8F509541787BF19B1C3D2BFEFD05D34C92",
        ),
    ]

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=2,
        ins_stmt=insert_sql_1,
        dp_ts=load_ts_4,
        current_ts=current_ts_4,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
        perform_merge_op=True,
        use_logical_delete_for_source_table=True,
        logical_delete_expression=DELTA_MODE_DELETE_EXPRESSION
    )
