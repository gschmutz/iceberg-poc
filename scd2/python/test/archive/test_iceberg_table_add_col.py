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
    get_strategy_name,
    insert_as_preparation,
    source_table_fqn,
    scd2_merge_as_test,
    scd2_sel_as_test,
)
from constants import MAX_TS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME = f"reports/{get_strategy_name().lower()}/test_iceberg_table_add_col.md"

load_ts_1 = datetime.strptime("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_1 = datetime.strptime("2026-01-02 00:00:00", "%Y-%m-%d %H:%M:%S")


def test_step_1(ctx):
    logger.info(
        "-------------------------------- Test Step 1 --------------------------------"
    )

    create_raw_table(ctx)

    render_init("Testing Add Column to existing Iceberg table", FILE_NAME)
    render_data(
        "This test validates an ALTER TABLE ADD COLUMN operation on an existing Iceberg table.",
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

    insert_as_preparation(ctx, [insert_sql])

    df_before = get_table_data(ctx.conn, f"{source_table_fqn(ctx)}", order_by_cols=[])
    render_table(
        df_before,
        title=f"Table {RAW_TABLE_NAME} before ADD COLUMN",
        output_file_name=FILE_NAME,
    )

    if get_strategy_name() == "TRINO":
        text_dt = "VARCHAR"
    else:
        text_dt = "STRING"  # Spark does not support TEXT type

    rename_stmt = f"""
                    ALTER TABLE {source_table_fqn(ctx)}
                    ADD COLUMN new_col {text_dt} AFTER email
                    """
    print(rename_stmt)
    render_data(f"Executing ADD COLUMN", output_file_name=FILE_NAME)

    update_sql = f"""
        UPDATE {source_table_fqn(ctx)}
        SET new_col = 'New Value'
    """
    insert_as_preparation(ctx, [rename_stmt, update_sql])

    # Run SELECT test
    test_description = f"Select all the latest data. Even though Bob has been deleted it will still be shown because we are selecting the latest records as of today."

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
            "alice.meyer@example.com",
            "New Value",
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
            "New Value",
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
            "New Value",
            "ACTIVE",
            load_ts_1,
            load_ts_1,
        ),
    ]

    scd2_sel_as_test(
        ctx,
        sel_stmt=sel_stmt,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
    )
