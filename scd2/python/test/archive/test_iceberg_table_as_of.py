import logging
import os
import sys
from datetime import date, datetime, timedelta

import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../lib")))
from util import (
    diff_with_color,
    get_credential,
    get_param,
    get_table_data,
    render_data,
    render_init,
    render_table,
    replace_vars_in_string,
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../lib")))
from commons import (
    COLS_WITH_TYPE,
    RAW_TABLE_NAME,
    S3_WAREHOUSE_BUCKET,
    S3_WAREHOUSE_PREFIX,
    TRINO_CATALOG,
    TRINO_SCHEMA,
    create_raw_table,
    execute_select,
    get_strategy_name,
    insert_as_preparation,
    source_table_fqn,
    scd2_merge_as_test,
    scd2_sel_as_test,
)
from constants import MAX_TS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME = f"reports/{get_strategy_name().lower()}/test_iceberg_table_as_of.md"

load_ts_1 = datetime.strptime("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_1 = datetime.strptime("2026-01-02 00:00:00", "%Y-%m-%d %H:%M:%S")


def test_step_1(ctx):
    logger.info(
        "-------------------------------- Test Step 1 --------------------------------"
    )

    create_raw_table(ctx)

    render_init("Testing Timetravel", FILE_NAME)
    render_data(
        "This test validates an SELECT ... FOR VERSION AS OF operation on an existing Iceberg table.",
        output_file_name=FILE_NAME,
    )
    render_data("\n", output_file_name=FILE_NAME)
    render_data(f" * **Strategy:** `{get_strategy_name().lower()}`", output_file_name=FILE_NAME)
    render_data(f" * **Last Run:** `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`", output_file_name=FILE_NAME)

    render_data(f"## Test Step 1", output_file_name=FILE_NAME)

    # Prepare --- Insert statements ---
    insert_sql = f"""
        INSERT INTO {source_table_fqn(ctx)}
        VALUES
            (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
    """

    # Prepare --- Update statement to add value to new_col ---
    update_sql = f"""
        UPDATE {source_table_fqn(ctx)}
        SET email = 'alice.meyer@newcorp.com'
        WHERE id = 1
    """

    insert_as_preparation(
        ctx=ctx,
        ins_stmts=[
            insert_sql,
            update_sql
        ],
    )

    # Run SELECT test
    sel_stmt = f"""
        SELECT * 
        FROM {source_table_fqn(ctx)}
        ORDER BY id
        """

    expected = [
        (
            1,
            "Alice",
            "Meyer",
            "Zurich",
            "alice.meyer@newcorp.com",
            "ACTIVE",
            load_ts_1,
            load_ts_1,
        ),
        (
            2,
            "Bob",
            "Keller",
            "Bern",
            "bob.keller@example.com",
            "ACTIVE",
            load_ts_1,
            load_ts_1,
        ),
        (
            3,
            "Clara",
            "Schmid",
            "Basel",
            "clara.schmid@example.com",
            "ACTIVE",
            load_ts_1,
            load_ts_1,
        ),
    ]

    # Run SELECT test
    test_description = f"Select all the latest data. Even though Bob has been deleted it will still be shown because we are selecting the latest records as of today."
    scd2_sel_as_test(
        ctx,
        sel_stmt=sel_stmt,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
    )


def test_step_2(ctx):
    logger.info(
        "-------------------------------- Test Step 2 --------------------------------"
    )

    sel_snapshot_id = f"""
        SELECT parent_id 
        FROM {source_table_fqn(ctx, iceberg_meta_tablename="snapshots")} 
        ORDER BY committed_at DESC
        LIMIT 1
        """
    snapshot_df = execute_select(sel_snapshot_id, ctx)
    snapshot_id = snapshot_df.iloc[0, 0]
    print(f"Using snapshot_id {snapshot_id} for SELECT ... FOR VERSION AS OF test")

    # Run SELECT test
    sel_stmt = f"""
        SELECT * 
        FROM {source_table_fqn(ctx)}
        FOR VERSION AS OF {snapshot_id}
        ORDER BY id
        """

    expected = [
        (
            1,
            "Alice",
            "Meyer",
            "Zurich",
            "alice.meyer@example.com",
            "ACTIVE",
            load_ts_1,
            load_ts_1,
        ),
        (
            2,
            "Bob",
            "Keller",
            "Bern",
            "bob.keller@example.com",
            "ACTIVE",
            load_ts_1,
            load_ts_1,
        ),
        (
            3,
            "Clara",
            "Schmid",
            "Basel",
            "clara.schmid@example.com",
            "ACTIVE",
            load_ts_1,
            load_ts_1,
        ),
    ]

    test_description = f"Select all the latest data. Even though Bob has been deleted it will still be shown because we are selecting the latest records as of today."
    scd2_sel_as_test(
        ctx,
        sel_stmt=sel_stmt,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
    )
