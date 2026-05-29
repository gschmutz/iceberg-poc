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
    scd2_merge_as_preparation,
    scd2_merge_as_test2,
)
from constants import MAX_TS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME = f"reports/{get_strategy_name().lower()}/scd2_test_replay.md"

load_ts_1 = datetime.strptime("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_1 = datetime.strptime("2026-01-02 00:00:00", "%Y-%m-%d %H:%M:%S")

load_ts_2 = datetime.strptime("2026-01-05 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_2 = datetime.strptime("2026-01-06 00:00:00", "%Y-%m-%d %H:%M:%S")

load_ts_3 = datetime.strptime("2026-01-10 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_3 = datetime.strptime("2026-01-11 00:00:00", "%Y-%m-%d %H:%M:%S")

load_ts_4 = datetime.strptime("2026-01-20 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_4 = datetime.strptime("2026-01-21 00:00:00", "%Y-%m-%d %H:%M:%S")


def test_step_1(ctx):
    logger.info(
        "-------------------------------- Test Step 1 --------------------------------"
    )

    create_raw_table(ctx)
    create_scd2_table_for_test(ctx)

    render_init("Testing Multiple Operations with a replay", FILE_NAME)
    render_data(
        "This test validates multiple operations on one entity over time producing many versions followed by a replay of these operations. This proves that the SCD2 operations are idempotent, so that the exact same result as before the replay is still in place.",
        output_file_name=FILE_NAME,
    )
    render_data("\n", output_file_name=FILE_NAME)
    render_data(f" * **Strategy:** `{get_strategy_name().lower()}`", output_file_name=FILE_NAME)
    render_data(f" * **Last Run:** `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`", output_file_name=FILE_NAME)

    test_description = (
        "Insert 2 entities into raw table and perform initial SCD2 merge."
    )

    # --- Insert statement (batch 1) ---
    insert_sql_1 = f"""
        INSERT INTO {source_table_fqn(ctx)}
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
                (1, 'Alice', 'Meyer', 'Bern', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}')
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
                (1, 'Alice', 'Meyer', 'Bern', 'alice.meyer@newmail.com', 'ACTIVE', TIMESTAMP '{load_ts_3}', TIMESTAMP '{load_ts_3}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_3}', TIMESTAMP '{load_ts_3}')
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

    # --- Insert statement (batch 4) ---
    insert_sql_4 = f"""
        INSERT INTO {source_table_fqn(ctx)}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Müller-Meyer', 'Bern', 'alice.meyer@newmail.com', 'ACTIVE', TIMESTAMP '{load_ts_4}', TIMESTAMP '{load_ts_4}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_4}', TIMESTAMP '{load_ts_4}')
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
    scd2_merge_as_preparation(
        ctx,
        ins_stmts=[insert_sql_1, insert_sql_2, insert_sql_3, insert_sql_4],
        dp_ts_list=[load_ts_1, load_ts_2, load_ts_3, load_ts_4],
        current_ts_list=[current_ts_1, current_ts_2, current_ts_3, current_ts_4],
        output_file_name=FILE_NAME,
    )


def test_step_4(ctx):
    logger.info(
        "-------------------------------- Test Step 4 --------------------------------"
    )

    test_description = f"At {load_ts_4}, update `last_name` of entity with `id=1` and perform SCD2 merge."

    expected = [
        (
            1,
            "Alice",
            "Meyer",
            "Zurich",
            "alice.meyer@example.com",
            load_ts_1,
            load_ts_2 - timedelta(seconds=1),
            False,
            False,
            current_ts_1,
            current_ts_2,
            "F244BC679F09400F6966D6472E66C079A59943FEC38344C4306149D8034ED570",
        ),
        (
            1,
            "Alice",
            "Meyer",
            "Bern",
            "alice.meyer@example.com",
            load_ts_2,
            load_ts_3 - timedelta(seconds=1),
            False,
            False,
            current_ts_2,
            current_ts_3,
            "59C89B7EF4747123AFC82ADCD3FC344DBFCDFAE429135184BB56925C4E542CD1",
        ),
        (
            1,
            "Alice",
            "Meyer",
            "Bern",
            "alice.meyer@newmail.com",
            load_ts_3,
            load_ts_4 - timedelta(seconds=1),
            False,
            False,
            current_ts_3,
            current_ts_4,
            "642FED688CC13719B3F12DF9A627FC85CEB15A0F4AF7FA4195D854426BD469BD",
        ),
        (
            1,
            "Alice",
            "Müller-Meyer",
            "Bern",
            "alice.meyer@newmail.com",
            load_ts_4,
            MAX_TS,
            True,
            True,
            current_ts_4,
            MAX_TS,
            "85284EE7F4FA5B634B7A213F8D197829E20937013F38B96477152FA0EC2632C6",
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
    ]

    # run test
    scd2_merge_as_test2(
        ctx,
        test_step=4,
        dp_ts_list=[load_ts_1, load_ts_2, load_ts_3, load_ts_4],
        current_ts_list=[current_ts_1, current_ts_2, current_ts_3, current_ts_4],
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
        perform_merge_op=True,
    )
