import logging
import os
import sys
from datetime import date, datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../lib")))
from util import (
    get_credential,
    get_param,
    render_data,
    render_init,
    replace_vars_in_string,
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../lib")))
from commons import (
    DELTA_MODE_DELETE_EXPRESSION,
    create_scd2_table_for_test,
    create_raw_table,
    get_strategy_name,
    source_table_fqn,
    scd2_intermediary_table_fqn,
    scd2_merge_as_preparation,
    scd2_merge_as_test,
    scd2_merge_as_test2,
    scd2_sel_as_test,
    scd2_table_fqn,
)
from constants import MAX_TS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME = "reports/scd2_test_del_with_many_versions.md"

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
    render_init("Testing Physical Delete Operation with many versions", FILE_NAME)
    render_data(
        "This test validates a DELETE operation of a single entity with many versions. The delete is created by a physical delete in the raw table, i.e., the entity is removed from the raw table partition.",
        output_file_name=FILE_NAME,
    )

    test_description = f"At {load_ts_1}, insert 3 entities into raw table and perform initial SCD2 merge. At {load_ts_2}, update email address of entity with `id=3` and perform SCD2 merge."

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

    scd2_merge_as_preparation(
        ctx,
        ins_stmts=[insert_sql_1],
        dp_ts_list=[load_ts_1],
        current_ts_list=[current_ts_1],
    )

    # --- Insert statement (batch 1) ---
    insert_sql_1 = f"""
        INSERT INTO {source_table_fqn(ctx)}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@newmail.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}')
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
            current_ts_2,
            "254677EA92F6E7E5A9C2629DE097CA5B2821DC6CF93B283D7158EC37920083CB",
        ),
        (
            3,
            "Clara",
            "Schmid",
            "Basel",
            "clara.schmid@newmail.com",
            load_ts_2,
            MAX_TS,
            True,
            True,
            current_ts_2,
            MAX_TS,
            "38315ECF14E9962283673DAFA2DFF3A93149B4C245591F3C6758E356EDBDE696",
        ),
    ]

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=1,
        ins_stmt=insert_sql_1,
        load_ts=load_ts_2,
        current_ts=current_ts_2,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
    )


def test_step_2(ctx):
    logger.info(
        "-------------------------------- Test Step 2 --------------------------------"
    )

    cursor = ctx.conn.cursor()

    test_description = f"At {load_ts_3}, delete entity with `id=3` from raw table (physical delete) and perform SCD2 merge. The active version of the entity with `id=3` should be marked as DELETED with `dp_ts_to` = current load timestamp - 1 second and `dp_is_active` = False."

    # --- Insert statement (batch 2) ---
    insert_sql_2 = f"""
        INSERT INTO {source_table_fqn(ctx)}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_3}', TIMESTAMP '{load_ts_3}'),
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
            current_ts_2,
            "254677EA92F6E7E5A9C2629DE097CA5B2821DC6CF93B283D7158EC37920083CB",
        ),
        (
            3,
            "Clara",
            "Schmid",
            "Basel",
            "clara.schmid@newmail.com",
            load_ts_2,
            load_ts_3 - timedelta(seconds=1),
            False,
            True,
            current_ts_2,
            current_ts_3,
            "38315ECF14E9962283673DAFA2DFF3A93149B4C245591F3C6758E356EDBDE696",
        ),
    ]

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=2,
        ins_stmt=insert_sql_2,
        load_ts=load_ts_3,
        current_ts=current_ts_3,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
        perform_merge_op=True,
    )
