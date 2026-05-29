import logging
import os
import sys
from datetime import date, datetime, timedelta

import pandas as pd

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

FILE_NAME = f"reports/{get_strategy_name().lower()}/scd2_test_sel_point_in_time.md"

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

    render_init("Testing for valid data at a given at a given timestamp", FILE_NAME)
    render_data(
        f"This test validates a single SELECT operation for data valid at a timestamp {load_ts_2 - timedelta(days=2)}",
        output_file_name=FILE_NAME,
    )
    render_data("\n", output_file_name=FILE_NAME)
    render_data(f" * **Strategy:** `{get_strategy_name().lower()}`", output_file_name=FILE_NAME)
    render_data(f" * **Last Run:** `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`", output_file_name=FILE_NAME)

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
    scd2_merge_as_preparation(
        ctx,
        ins_stmts=[insert_sql_1, insert_sql_2],
        dp_ts_list=[load_ts_1, load_ts_2],
        current_ts_list=[current_ts_1, current_ts_2],
        output_file_name=FILE_NAME,
    )

    # Run SELECT test
    test_description = f"Select data valid at {load_ts_2 - timedelta(days=2)}. As we are selecting back in time the old version of Alice is shown where she lived in Zurich."

    sel_stmt = f"""
        SELECT id, first_name, last_name, city, email,
                dp_ts_from, dp_ts_to, dp_is_active, dp_is_latest,
                dp_load_ts, dp_replace_ts,
                dp_record_hash  
        FROM {scd2_table_fqn(ctx)}
        WHERE TIMESTAMP '{load_ts_2}' - INTERVAL '2' DAY BETWEEN dp_ts_from AND dp_ts_to
        ORDER BY id
        """

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
            "00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8",
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
            "D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40",
        ),
    ]

    scd2_sel_as_test(
        ctx,
        sel_stmt=sel_stmt,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
    )
