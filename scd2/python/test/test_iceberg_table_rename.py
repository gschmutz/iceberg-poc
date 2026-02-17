import sys
import os
import logging
import numpy as np

from datetime import date, timedelta, datetime
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_init, render_table, render_data, get_table_data, diff_with_color
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from scd2 import merge_into_dim_table, optimize_table
from constants import MAX_TS
from commons import TRINO_CATALOG, TRINO_SCHEMA, S3_WAREHOUSE_BUCKET, S3_WAREHOUSE_PREFIX, RAW_TABLE_NAME, COLS_WITH_TYPE, scd2_merge_as_test, create_raw_table, init_trino_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME="reports/test_iceberg_table_rename.md"

load_ts_1= datetime.strptime('2026-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_1 = datetime.strptime('2026-01-02 00:00:00', '%Y-%m-%d %H:%M:%S')

conn = init_trino_connection()

def test_step_1():
    logger.info("-------------------------------- Test Step 1 --------------------------------")

    create_raw_table(conn)
    conn.cursor().execute(f"""DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}_renamed""")

    render_init("Testing Insert Operation", FILE_NAME)
    render_data("This test validates an INSERT operation of one new record", output_file_name=FILE_NAME)

    render_data(f"## Test Step 1", output_file_name=FILE_NAME)

    # Prepare --- Insert statements ---
    insert_sql = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        VALUES
            (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
            (3, 'Clara', 'Schmid', 'Basel', 'clara.schmid@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
    """

    conn.cursor().execute(insert_sql)

    df_before = get_table_data(conn, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}", order_by_cols=[])
    render_table(df_before, title=f"### Table {RAW_TABLE_NAME}", output_file_name=FILE_NAME)

    rename_stmt = f"""
                    ALTER TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
                    RENAME TO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}_renamed
                    """
    print(rename_stmt)
    render_data(f"Executing RENAME of `{RAW_TABLE_NAME}` to `{RAW_TABLE_NAME}_renamed`", output_file_name=FILE_NAME)
    conn.cursor().execute(rename_stmt)

    df_after = get_table_data(conn, f'{TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}_renamed')
    render_table(df_after, title=f"### Table {RAW_TABLE_NAME}_renamed", output_file_name=FILE_NAME)

    arr1 = df_after.to_numpy()
    arr2 = df_before.to_numpy()
    np.testing.assert_array_equal(arr1, arr2)