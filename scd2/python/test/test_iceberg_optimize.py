import sys
import os
import logging
from datetime import date, timedelta, datetime
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_init, render_table, render_data, get_table_data, diff_with_color
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from constants import MAX_TS
from commons import TRINO_CATALOG, TRINO_SCHEMA, S3_WAREHOUSE_BUCKET, S3_WAREHOUSE_PREFIX, RAW_TABLE_NAME, COLS_WITH_TYPE, scd2_merge_as_test, create_raw_table, optimize_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME="reports/test_iceberg_optimize.md"

load_ts_1= datetime.strptime('2026-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_1 = datetime.strptime('2026-01-02 00:00:00', '%Y-%m-%d %H:%M:%S')

#@pytest.fixture(autouse=True, scope="session")
#def setup_data(request):
#    create_raw_table()
#    create_dim_table(conn, TRINO_CATALOG, TRINO_SCHEMA, "{DIM_TABLE_NAME}", s3_warehouse_bucket=S3_WAREHOUSE_BUCKET, s3_warehouse_prefix=S3_WAREHOUSE_PREFIX, pk_col_with_type="id INT", cols_with_type=cols_with_type, partition_cols=["dp_ts_from"], sort_cols=[])
#    yield
#    logger.info("Finished all tests")


def test_step_1(trino_conn, spark):
    logger.info("-------------------------------- Test Step 1 --------------------------------")

    create_raw_table(trino_conn)
    render_init("Testing Insert Operation", FILE_NAME)
    render_data("This test validates an INSERT operation of one new record", output_file_name=FILE_NAME)

    render_data(f"## Test Step 1", output_file_name=FILE_NAME)

    # Prepare --- Insert statements ---
    insert_sql_1 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        VALUES
            (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
    """

    insert_sql_2 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        VALUES
            (10, 'David', 'Fischer', 'Lucerne', 'david.fischer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (12, 'Emma', 'Weber', 'Geneva', 'emma.weber@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (13, 'Felix', 'Moser', 'Lausanne', 'felix.moser@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
    """

    insert_sql_3 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        VALUES
            (21, 'Hannah', 'Roth', 'St. Gallen', 'hannah.roth@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (22, 'Ivan', 'Baumann', 'Winterthur', 'ivan.baumann@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (23, 'Julia', 'Hofer', 'Thun', 'julia.hofer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
    """

    insert_sql_4 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        VALUES
            (31, 'Klaus', 'Vogel', 'Zug', 'klaus.vogel@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (32, 'Laura', 'Meier', 'Schaffhausen', 'laura.meier@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (33, 'Martin', 'Gut', 'Aarau', 'martin.gut@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
    """

    insert_sql_5 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        VALUES
            (41, 'Nina', 'Steiner', 'Chur', 'nina.steiner@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (42, 'Oliver', 'Brunner', 'Sion', 'oliver.brunner@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (43, 'Paula', 'Gerber', 'Fribourg', 'paula.gerber@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
    """

    trino_conn.cursor().execute(insert_sql_1)
    trino_conn.cursor().execute(insert_sql_2)
    trino_conn.cursor().execute(insert_sql_3)
    trino_conn.cursor().execute(insert_sql_4)
    trino_conn.cursor().execute(insert_sql_5)

    df = get_table_data(trino_conn, f'{TRINO_CATALOG}.{TRINO_SCHEMA}."{RAW_TABLE_NAME}$files"', order_by_cols=[])
    render_table(df, title=f"### Iceberg Metadata before OPTIMIZE", include_cols=["file_path", "record_count", "file_size_in_bytes"], output_file_name=FILE_NAME)

    # Run system under test
    render_data("Executing OPTIMIZE on the Iceberg table.", output_file_name=FILE_NAME)
    optimize_table(trino_conn, spark, table_name=f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}")

    # Verify and Visualize results
    df = get_table_data(trino_conn, f'{TRINO_CATALOG}.{TRINO_SCHEMA}."{RAW_TABLE_NAME}$files"', order_by_cols=[])
    render_table(df, title=f"### Iceberg Metadata after OPTIMIZE", include_cols=["file_path", "record_count", "file_size_in_bytes"], output_file_name=FILE_NAME)

    assert len(df) == 1, f"Expected 1 file after OPTIMIZE, but found {len(df)}"