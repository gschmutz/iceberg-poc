import sys
import os
import logging
from datetime import date, timedelta, datetime
import logging
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from util import get_param, get_credential, replace_vars_in_string, render_init, render_data, get_table_data, render_table
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from scd2 import merge_into_dim_table, create_dim_table
from constants import MAX_TS
from commons import TRINO_CATALOG, TRINO_SCHEMA, S3_WAREHOUSE_BUCKET, S3_WAREHOUSE_PREFIX, DIM_TABLE_NAME, RAW_TABLE_NAME, SCD2_VIEW_NAME, EXCLUDE_COLS, COLS_WITH_TYPE, run_scd2_merge, run_scd2_merge_test, run_scd2_merge, create_raw_table, init_trino_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME="reports/scd2_test_sel_is_active.md"

load_ts_1= datetime.strptime('2026-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_1 = datetime.strptime('2026-01-02 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_2 = datetime.strptime('2026-01-05 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_2 = datetime.strptime('2026-01-06 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_3 = datetime.strptime('2026-01-10 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_3 = datetime.strptime('2026-01-11 00:00:00', '%Y-%m-%d %H:%M:%S')

load_ts_4 = datetime.strptime('2026-01-20 00:00:00', '%Y-%m-%d %H:%M:%S')
current_ts_4 = datetime.strptime('2026-01-21 00:00:00', '%Y-%m-%d %H:%M:%S')

conn = init_trino_connection()

#@pytest.fixture(autouse=True, scope="session")
#def setup_data(request):
#    create_raw_table()
#    create_dim_table(conn, TRINO_CATALOG, TRINO_SCHEMA, "{DIM_TABLE_NAME}", s3_warehouse_bucket=S3_WAREHOUSE_BUCKET, s3_warehouse_prefix=S3_WAREHOUSE_PREFIX, pk_col_with_type="id INT", cols_with_type=cols_with_type, partition_cols=["dp_valid_from"], sort_cols=[])
#    yield
#    logger.info("Finished all tests")


def test_step_1():
    logger.info("-------------------------------- Test Step 1 --------------------------------")

    create_raw_table(conn)
    create_dim_table(conn, TRINO_CATALOG, TRINO_SCHEMA, DIM_TABLE_NAME, s3_warehouse_bucket=S3_WAREHOUSE_BUCKET, s3_warehouse_prefix=S3_WAREHOUSE_PREFIX, pk_col_with_type="id INT", cols_with_type=COLS_WITH_TYPE, partition_cols=["dp_valid_from"], sort_cols=[])
    
    render_init("Testing for valid data at a given at a given timestamp", FILE_NAME)
    render_data("This test validates multiple UPDATE operations on one entity over time producing many versions.", output_file_name=FILE_NAME)

    test_description = "Insert 2 entities into raw table, perform initial SCD2 merge and then do an update."

    # --- Insert statement (batch 1) ---
    insert_sql_1 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_1}')
        ) AS t (
            id,
            first_name,
            last_name,
            city,
            email,
            status,
            dp_exported_at
        )
    """
    run_scd2_merge(conn, ins_stmt=insert_sql_1, load_ts=load_ts_1, current_ts=current_ts_1, perform_merge_op=True)

    # --- Insert statement (batch 2) ---
    insert_sql_2 = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{RAW_TABLE_NAME}
        SELECT *
        FROM (
            VALUES
                (1, 'Alice', 'Meyer', 'Bern', 'alice.meyer@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}'),
                (2, 'Bob', 'Keller', 'Bern', 'bob.keller@example.com', 'ACTIVE', TIMESTAMP '{load_ts_2}')
            ) AS t (
            id,
            first_name,
            last_name,
            city,
            email,
            status,
            dp_exported_at
        )
    """
    expected = [
        (1, "Alice", "Meyer", "Zurich", "alice.meyer@example.com",
        load_ts_1, load_ts_2 - timedelta(seconds=1), False, False,
        current_ts_1, current_ts_1, current_ts_2,
        "SUPERSEDED", "FF118EED04F8A2D0133E79435F7BC3CEBC0011D256A07FE02953CD12B3E29E51"),

        (1, "Alice", "Meyer", "Bern", "alice.meyer@example.com",
        load_ts_2, MAX_TS, True, True,
        current_ts_2, current_ts_2, MAX_TS,
        "SUPERSEDED_BY", "67B1EB7F635FBBC16C2FFA0EAD786E929C4D1F8E26B210ABFE37D0CFB73EDE39"),

        (2, "Bob", "Keller", "Bern", "bob.keller@example.com",
        load_ts_1, MAX_TS, True, True,
        current_ts_1, current_ts_1, MAX_TS,
        "NEW", "68844625A41E2D2540D4A17FBC7B51B3733C95FC58817DA05765F111F4F659CE")
    ]

    # run test
    run_scd2_merge_test(conn, test_step=2, ins_stmt=insert_sql_2, load_ts=load_ts_2, current_ts=current_ts_2, expected=expected, output_file_name=FILE_NAME, test_description=test_description)

    test_description = f"Select active versions of all entities."

    # Run SELECT test
    sel_stmt = f"""
        SELECT * 
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{DIM_TABLE_NAME}
        WHERE dp_is_active = TRUE
        """
    print (sel_stmt)
    df = pd.read_sql_query(sel_stmt, conn)
    render_table(df, title=f"### Dim Table `{DIM_TABLE_NAME}` Result", output_file_name=FILE_NAME)