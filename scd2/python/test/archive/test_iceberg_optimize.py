import logging
import os
import sys
from datetime import date, datetime, timedelta
from scd2_strategy import SCD2Table

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../lib")))
from util import (
    diff_with_color,
    get_credential,
    get_param,
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
    get_table_data,
    insert_as_preparation,
    optimize_table,
    raw_table_fqn,
    scd2_merge_as_test,
)
from constants import MAX_TS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME = f"reports/{get_strategy_name().lower()}/test_iceberg_optimize.md"

load_ts_1 = datetime.strptime("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_1 = datetime.strptime("2026-01-02 00:00:00", "%Y-%m-%d %H:%M:%S")


def test_step_1(ctx):
    logger.info(
        "-------------------------------- Test Step 1 --------------------------------"
    )

    create_raw_table(ctx)
    render_init("Testing Insert Operation", FILE_NAME)
    render_data(
        "This test validates an INSERT operation of one new record",
        output_file_name=FILE_NAME,
    )
    render_data("\n", output_file_name=FILE_NAME)
    render_data(f" * **Strategy:** `{get_strategy_name().lower()}`", output_file_name=FILE_NAME)
    render_data(f" * **Last Run:** `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`", output_file_name=FILE_NAME)

    render_data(f"## Test Step 1", output_file_name=FILE_NAME)

    # Prepare --- Insert statements ---
    insert_sql_1 = f"""
        INSERT INTO {raw_table_fqn(ctx)}
        VALUES
            (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
    """

    insert_sql_2 = f"""
        INSERT INTO {raw_table_fqn(ctx)}
        VALUES
            (10, 'David', 'Fischer', 'Lucerne', 'david.fischer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (12, 'Emma', 'Weber', 'Geneva', 'emma.weber@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (13, 'Felix', 'Moser', 'Lausanne', 'felix.moser@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
    """

    insert_sql_3 = f"""
        INSERT INTO {raw_table_fqn(ctx)}
        VALUES
            (21, 'Hannah', 'Roth', 'St. Gallen', 'hannah.roth@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (22, 'Ivan', 'Baumann', 'Winterthur', 'ivan.baumann@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (23, 'Julia', 'Hofer', 'Thun', 'julia.hofer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
    """

    insert_sql_4 = f"""
        INSERT INTO {raw_table_fqn(ctx)}
        VALUES
            (31, 'Klaus', 'Vogel', 'Zug', 'klaus.vogel@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (32, 'Laura', 'Meier', 'Schaffhausen', 'laura.meier@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (33, 'Martin', 'Gut', 'Aarau', 'martin.gut@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
    """

    insert_sql_5 = f"""
        INSERT INTO {raw_table_fqn(ctx)}
        VALUES
            (41, 'Nina', 'Steiner', 'Chur', 'nina.steiner@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (42, 'Oliver', 'Brunner', 'Sion', 'oliver.brunner@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (43, 'Paula', 'Gerber', 'Fribourg', 'paula.gerber@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
    """

    insert_as_preparation(
        ctx=ctx,
        ins_stmts=[
            insert_sql_1,
            insert_sql_2,
            insert_sql_3,
            insert_sql_4,
            insert_sql_5,
        ],
    )

    df = get_table_data(
        ctx=ctx, table=SCD2Table.RAW, iceberg_meta_tablename="files", order_by_cols=[]
    )
    render_table(
        df,
        title=f"### Iceberg Metadata before OPTIMIZE",
        include_cols=["file_path", "record_count", "file_size_in_bytes"],
        output_file_name=FILE_NAME,
    )

    # Run system under test
    render_data("Executing OPTIMIZE on the Iceberg table.", output_file_name=FILE_NAME)
    optimize_table(ctx, table_name=raw_table_fqn(ctx))

    # Verify and Visualize results
    df = get_table_data(ctx=ctx, table=SCD2Table.SCD2, iceberg_meta_tablename="files", order_by_cols=[])
    render_table(
        df,
        title=f"### Iceberg Metadata after OPTIMIZE",
        include_cols=["file_path", "record_count", "file_size_in_bytes"],
        output_file_name=FILE_NAME,
    )

    assert len(df) == 2, f"Expected 2 files after OPTIMIZE, but found {len(df)}"
