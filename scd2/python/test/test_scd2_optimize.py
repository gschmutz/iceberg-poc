import logging
import os
import sys
from datetime import datetime

import pandas as pd
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../lib")))
from util import (
    render_data,
    render_init,
    render_table,
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../lib")))
from commons import (
    _impl,
    create_scd2_table_for_test,
    create_raw_table,
    get_strategy_name,
    get_table_data,
    source_table_fqn,
    scd2_merge_as_test,
    scd2_table_fqn,
)
from lib.scd2_strategy import SCD2Table
from constants import MAX_TS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME = f"reports/{get_strategy_name().lower()}/scd2_test_optimize.md"

load_ts_1 = datetime.strptime("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_1 = datetime.strptime("2026-01-02 00:00:00", "%Y-%m-%d %H:%M:%S")

load_ts_2 = datetime.strptime("2026-01-05 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_2 = datetime.strptime("2026-01-06 00:00:00", "%Y-%m-%d %H:%M:%S")


def test_step_1(ctx):
    """Seed the SCD2 table with initial data via two merge batches."""
    logger.info(
        "-------------------------------- Test Step 1 --------------------------------"
    )

    create_raw_table(ctx)
    create_scd2_table_for_test(ctx)
    render_init("Testing optimize_table Operation", FILE_NAME)
    render_data(
        "This test validates the `optimize_table` method, which compacts small Iceberg "
        "data files produced by repeated MERGE operations into fewer, larger files.\n\n"
        "The test seeds the SCD2 table via two merge batches so that multiple small "
        "data files are present before optimization is triggered.",
        output_file_name=FILE_NAME,
    )
    render_data("\n", output_file_name=FILE_NAME)
    render_data(f" * **Strategy:** `{get_strategy_name().lower()}`", output_file_name=FILE_NAME)
    render_data(f" * **Last Run:** `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`", output_file_name=FILE_NAME)

    test_description = "Insert 3 entities into raw table and perform initial SCD2 merge (batch 1)."

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
            1, "Alice", "Meyer", "Zurich", "alice.meyer@example.com",
            load_ts_1, MAX_TS, True, True, current_ts_1, MAX_TS,
            "00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8",
        ),
        (
            2, "Bob", "Keller", "Bern", "bob.keller@example.com",
            load_ts_1, MAX_TS, True, True, current_ts_1, MAX_TS,
            "D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40",
        ),
        (
            3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
            load_ts_1, MAX_TS, True, True, current_ts_1, MAX_TS,
            "77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676",
        ),
    ]

    scd2_merge_as_test(
        ctx,
        test_step=1,
        ins_stmt=insert_sql_1,
        dp_ts=load_ts_1,
        current_ts=current_ts_1,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
    )


def test_step_2(ctx):
    """Add a new entity in batch 2, creating additional small files."""
    logger.info(
        "-------------------------------- Test Step 2 --------------------------------"
    )

    test_description = (
        f"At {load_ts_2}, add entity `id=10` and re-submit existing entities "
        "(no-ops) to produce a second set of Iceberg data files."
    )

    insert_sql_2 = f"""
        INSERT INTO {source_table_fqn(ctx)}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (10, 'Kevin', 'Loosli', 'Bern', 'kevin.loosli@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}')
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
            1, "Alice", "Meyer", "Zurich", "alice.meyer@example.com",
            load_ts_1, MAX_TS, True, True, current_ts_1, MAX_TS,
            "00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8",
        ),
        (
            2, "Bob", "Keller", "Bern", "bob.keller@example.com",
            load_ts_1, MAX_TS, True, True, current_ts_1, MAX_TS,
            "D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40",
        ),
        (
            3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
            load_ts_1, MAX_TS, True, True, current_ts_1, MAX_TS,
            "77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676",
        ),
        (
            10, "Kevin", "Loosli", "Bern", "kevin.loosli@example.com",
            load_ts_2, MAX_TS, True, True, current_ts_2, MAX_TS,
            "F32E425B7483AA533A0DBD8DB41BBD3DEEDBD2FF6427D420A7130EC9B174787C",
        ),
    ]

    scd2_merge_as_test(
        ctx,
        test_step=2,
        ins_stmt=insert_sql_2,
        dp_ts=load_ts_2,
        current_ts=current_ts_2,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
        perform_merge_op=True,
    )


EXPECTED_AFTER_STEP_2 = [
    (
        1, "Alice", "Meyer", "Zurich", "alice.meyer@example.com",
        load_ts_1, MAX_TS, True, True, current_ts_1, MAX_TS,
        "00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8",
    ),
    (
        2, "Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True, current_ts_1, MAX_TS,
        "D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40",
    ),
    (
        3, "Clara", "Schmid", "Basel", "clara.schmid@example.com",
        load_ts_1, MAX_TS, True, True, current_ts_1, MAX_TS,
        "77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676",
    ),
    (
        10, "Kevin", "Loosli", "Bern", "kevin.loosli@example.com",
        load_ts_2, MAX_TS, True, True, current_ts_2, MAX_TS,
        "F32E425B7483AA533A0DBD8DB41BBD3DEEDBD2FF6427D420A7130EC9B174787C",
    ),
]


def _assert_data_unchanged(ctx, test_step: int, description: str):
    """Read the SCD2 table and assert it matches EXPECTED_AFTER_STEP_2."""
    actual_df = get_table_data(
        ctx,
        SCD2Table.SCD2,
        order_by_cols=["id", "dp_ts_from"],
        exclude_cols=["dp_record_id"],
    )
    render_data(f"## Test Step {test_step}", output_file_name=FILE_NAME)
    render_data(description, output_file_name=FILE_NAME)
    render_table(actual_df, title=f"Dimensional Table after step {test_step}", output_file_name=FILE_NAME)
    expected_df = pd.DataFrame(EXPECTED_AFTER_STEP_2, columns=actual_df.columns)
    _impl.assert_df(actual_df, expected_df, output_file_name=FILE_NAME)


def test_step_3_optimize_no_threshold(ctx):
    """Call optimize_table on the SCD2 table without a file-size threshold."""
    logger.info(
        "-------------------------------- Test Step 3 --------------------------------"
    )

    _impl._make_strategy(ctx).optimize_table()

    _assert_data_unchanged(
        ctx,
        test_step=3,
        description=(
            "Call `optimize_table()` on the SCD2 dimension table without specifying "
            "a file-size threshold (engine default is used).  Verifies that the call "
            "completes without error and that the table data is unchanged afterward."
        ),
    )


def test_step_4_optimize_with_threshold(ctx):
    """Call optimize_table with an explicit file-size threshold of 256 MB."""
    logger.info(
        "-------------------------------- Test Step 4 --------------------------------"
    )

    _impl._make_strategy(ctx).optimize_table(file_size_threshold="256MB")

    _assert_data_unchanged(
        ctx,
        test_step=4,
        description=(
            "Call `optimize_table(file_size_threshold='256MB')` and verify data is "
            "intact — compaction must not alter any rows."
        ),
    )
