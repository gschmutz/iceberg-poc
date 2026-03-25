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
    raw_table_fqn,
    scd2_merge_as_test,
)
from constants import MAX_TS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME = f"reports/{get_strategy_name().lower()}/test_iceberg_table_rename.md"

load_ts_1 = datetime.strptime("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_1 = datetime.strptime("2026-01-02 00:00:00", "%Y-%m-%d %H:%M:%S")


def test_step_1(ctx):
    logger.info(
        "-------------------------------- Test Step 1 --------------------------------"
    )

    create_raw_table(ctx)
    ctx.conn.cursor().execute(
        f"""DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}_renamed"""
    )

    render_init("Testing Insert Operation", FILE_NAME)
    render_data(
        "This test validates an INSERT operation of one new record",
        output_file_name=FILE_NAME,
    )

    render_data(f"## Test Step 1", output_file_name=FILE_NAME)

    # Prepare --- Insert statements ---
    insert_sql = f"""
        INSERT INTO {raw_table_fqn(ctx)}
        VALUES
            (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
    """

    ctx.conn.cursor().execute(insert_sql)

    df_before = get_table_data(
        ctx.conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}", order_by_cols=[]
    )
    render_table(
        df_before, title=f"### Table {RAW_TABLE_NAME}", output_file_name=FILE_NAME
    )

    rename_stmt = f"""
                    ALTER TABLE {raw_table_fqn(ctx)}
                    RENAME TO {raw_table_fqn(ctx)}_renamed
                    """
    print(rename_stmt)
    render_data(
        f"Executing RENAME of `{RAW_TABLE_NAME}` to `{RAW_TABLE_NAME}_renamed`",
        output_file_name=FILE_NAME,
    )
    ctx.conn.cursor().execute(rename_stmt)

    df_after = get_table_data(ctx.conn, f"{raw_table_fqn(ctx)}_renamed")
    render_table(
        df_after,
        title=f"### Table {raw_table_fqn(ctx)}_renamed",
        output_file_name=FILE_NAME,
    )

    arr1 = df_after.to_numpy()
    arr2 = df_before.to_numpy()
    np.testing.assert_array_equal(arr1, arr2)
