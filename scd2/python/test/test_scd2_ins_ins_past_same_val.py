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
    create_scd2_table_for_test,
    create_raw_table,
    get_strategy_name,
    source_table_fqn,
    scd2_merge_as_test,
)
from constants import MAX_TS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME = f"reports/{get_strategy_name().lower()}/scd2_test_ins_ins_past_same_val.md"

load_ts_1 = datetime.strptime("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_1 = datetime.strptime("2026-01-02 00:00:00", "%Y-%m-%d %H:%M:%S")

load_ts_2 = datetime.strptime("2026-01-05 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_2 = datetime.strptime("2026-01-06 00:00:00", "%Y-%m-%d %H:%M:%S")

load_ts_3 = datetime.strptime("2026-01-10 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_3 = datetime.strptime("2026-01-11 00:00:00", "%Y-%m-%d %H:%M:%S")


def test_step_1(ctx):
    logger.info(
        "-------------------------------- Test Step 1 --------------------------------"
    )

    create_raw_table(ctx)
    create_scd2_table_for_test(ctx)
    render_init(
        "Testing Insert Operation followed by an Insert Operation in the past",
        FILE_NAME,
    )
    render_data(
        "This test validates an INSERT operation in the past of a version for an entity which already exists and with the same value.",
        output_file_name=FILE_NAME,
    )
    render_data("\n", output_file_name=FILE_NAME)
    render_data(f" * **Strategy:** `{get_strategy_name().lower()}`", output_file_name=FILE_NAME)
    render_data(f" * **Last Run:** `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`", output_file_name=FILE_NAME)

    test_description = f"At {load_ts_2}, insert 3 entities into raw table and perform initial SCD2 merge."

    # --- Insert statement (batch 1) ---
    insert_sql_1 = f"""
        INSERT INTO {source_table_fqn(ctx)}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
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
        (
            1,
            "Alice",
            "Meyer",
            "Zurich",
            "alice.meyer@example.com",
            load_ts_2,
            MAX_TS,
            True,
            True,
            current_ts_2,
            MAX_TS,
            "F244BC679F09400F6966D6472E66C079A59943FEC38344C4306149D8034ED570",
        ),
        (
            2,
            "Bob",
            "Keller",
            "Bern",
            "bob.keller@example.com",
            load_ts_2,
            MAX_TS,
            True,
            True,
            current_ts_2,
            MAX_TS,
            "E5181C5926D5185F99D4654A7F15E2476045F4808F22C5924E128B87DEB6F93F",
        ),
        (
            3,
            "Clara",
            "Schmid",
            "Basel",
            "clara.schmid@example.com",
            load_ts_2,
            MAX_TS,
            True,
            True,
            current_ts_2,
            MAX_TS,
            "254677EA92F6E7E5A9C2629DE097CA5B2821DC6CF93B283D7158EC37920083CB",
        ),
    ]

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=1,
        ins_stmt=insert_sql_1,
        dp_ts=load_ts_2,
        current_ts=current_ts_2,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
    )


def test_step_2(ctx):
    logger.info(
        "-------------------------------- Test Step 2 --------------------------------"
    )

    test_description = f"At {load_ts_1}, insert the entity with `id=1` into the new partitions of the raw table and perform SCD2 merge."

    # --- Insert statement (batch 2) ---
    insert_sql_2 = f"""
        INSERT INTO {source_table_fqn(ctx)}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_3}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_3}'),
                (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_3}')
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
            current_ts_2,
            current_ts_3,
            "F244BC679F09400F6966D6472E66C079A59943FEC38344C4306149D8034ED570",
        ),
        (
            2,
            "Bob",
            "Keller",
            "Bern",
            "bob.keller@example.com",
            load_ts_2,
            MAX_TS,
            True,
            True,
            current_ts_2,
            MAX_TS,
            "E5181C5926D5185F99D4654A7F15E2476045F4808F22C5924E128B87DEB6F93F",
        ),
        (
            3,
            "Clara",
            "Schmid",
            "Basel",
            "clara.schmid@example.com",
            load_ts_2,
            MAX_TS,
            True,
            True,
            current_ts_2,
            MAX_TS,
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
        perform_merge_op=True,
    )
