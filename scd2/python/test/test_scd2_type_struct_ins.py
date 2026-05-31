import logging
import logging
import os
import sys
from datetime import date, datetime, timedelta

import pytest

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
from common_utils import dict_to_tuple_in_rows
from constants import MAX_TS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME = f"reports/{get_strategy_name().lower()}/scd2_test_type_struct_ins.md"

load_ts_1 = datetime.strptime("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_1 = datetime.strptime("2026-01-02 00:00:00", "%Y-%m-%d %H:%M:%S")

load_ts_2 = datetime.strptime("2026-01-05 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_2 = datetime.strptime("2026-01-06 00:00:00", "%Y-%m-%d %H:%M:%S")

TEST_ENGINE = os.getenv("TEST_ENGINE", "SPARK").upper()
OBJECT_DATATYPE = "STRUCT" if (TEST_ENGINE == "SPARK" or TEST_ENGINE == "PYSPARK") else "ROW"

def test_step_1(ctx):
    logger.info(
        "-------------------------------- Test Step 1 --------------------------------"
    )

    create_raw_table(ctx, table_shape="struct")
    create_scd2_table_for_test(ctx, table_shape="struct")
    render_init("Testing Insert Operation", FILE_NAME)
    render_data(
        "This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities.",
        output_file_name=FILE_NAME,
    )
    render_data("\n", output_file_name=FILE_NAME)
    render_data(f" * **Strategy:** `{get_strategy_name().lower()}`", output_file_name=FILE_NAME)
    render_data(f" * **Last Run:** `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`", output_file_name=FILE_NAME)

    test_description = (
        "Insert 3 entities into raw table and perform initial SCD2 merge."
    )

    # --- Insert statement (batch 1) ---
    insert_sql_1 = f"""
        INSERT INTO {source_table_fqn(ctx)}
        SELECT *
        FROM (
            VALUES
                (1, {OBJECT_DATATYPE}('Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com'), 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (2, {OBJECT_DATATYPE}('Bob', 'Keller', 'Bern', 'bob.keller@example.com'), 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (3, {OBJECT_DATATYPE}('Clara', 'Schmid', 'Basel', 'clara.schmid@example.com'), 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
        ) AS t (
            id,
            user_info,
            status,
            dp_ts_from,
            dp_loaded_at
        )
    """

    expected = [
        (
            1,
            {"first_name": "Alice", "last_name": "Meyer", "city": "Zurich", "email": "alice.meyer@example.com"},  # user_info
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "F379F42F805729CFC146CDA0164C5E898C7AD060C07D291F692DA97B3823A7D9",
        ),
        (
            2,
            {"first_name": "Bob", "last_name": "Keller", "city": "Bern", "email": "bob.keller@example.com"},      # user_info
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "228993688DBB10E60E1CAC8F1D0AA141FDEADB2FC38B9C8A3483DC61655F6B3B",
        ),
        (
            3,
            {"first_name": "Clara", "last_name": "Schmid", "city": "Basel", "email": "clara.schmid@example.com"}, # user_info
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "42E013F11875C7C485E0080FFD38BE6C27A978342DE9346F1D8CD189BF0894E0",
        ),
    ]
    
    if (TEST_ENGINE == "TRINO"):
        expected = dict_to_tuple_in_rows(expected)

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=1,
        ins_stmt=insert_sql_1,
        table_shape="struct",
        dp_ts=load_ts_1,
        current_ts=current_ts_1,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
    )


# @pytest.mark.skip(reason="not implemented yet")
def test_step_2(ctx):
    logger.info(
        "-------------------------------- Test Step 2 --------------------------------"
    )

    test_description = f"At {load_ts_2}, insert the new entity with `id=10` into the new partition of the raw table and perform SCD2 merge."

    # --- Insert statement (batch 2) ---
    insert_sql_2 = f"""
        INSERT INTO {source_table_fqn(ctx)}
        SELECT *
        FROM (
            VALUES
                (1,  {OBJECT_DATATYPE}('Alice', 'Meyer',  'Zurich', 'alice.meyer@example.com'),  'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (2,  {OBJECT_DATATYPE}('Bob',   'Keller', 'Bern',   'bob.keller@example.com'),   'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (3,  {OBJECT_DATATYPE}('Clara', 'Schmid', 'Basel',  'clara.schmid@example.com'), 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (10, {OBJECT_DATATYPE}('Kevin', 'Loosli', 'Bern',   'kevin.loosli@example.com'), 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}')
        ) AS t (
            id,
            user_info,
            status,
            dp_ts_from,
            dp_loaded_at
        )
    """

    expected = [
        (
            1,
            {"first_name": "Alice", "last_name": "Meyer", "city": "Zurich", "email": "alice.meyer@example.com"},  # user_info (no status)
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "F379F42F805729CFC146CDA0164C5E898C7AD060C07D291F692DA97B3823A7D9",
        ),
        (
            2,
            {"first_name": "Bob", "last_name": "Keller", "city": "Bern", "email": "bob.keller@example.com"},      # user_info (no status)
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "228993688DBB10E60E1CAC8F1D0AA141FDEADB2FC38B9C8A3483DC61655F6B3B",
        ),
        (
            3,
            {"first_name": "Clara", "last_name": "Schmid", "city": "Basel", "email": "clara.schmid@example.com"}, # user_info (no status)
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "42E013F11875C7C485E0080FFD38BE6C27A978342DE9346F1D8CD189BF0894E0",
        ),
        (
            10,
            {"first_name": "Kevin", "last_name": "Loosli", "city": "Bern", "email": "kevin.loosli@example.com"},  # user_info (no status)
            load_ts_2,
            MAX_TS,
            True,
            True,
            current_ts_2,
            MAX_TS,
            "BBDC7F9E78A87D2F0CCE5106F1735616B93F60FA3C442BFC2A792BFAA11F3CC1",
        ),
    ]

    if (TEST_ENGINE == "TRINO"):
        expected = dict_to_tuple_in_rows(expected)

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=2,
        ins_stmt=insert_sql_2,
        table_shape="struct",
        dp_ts=load_ts_2,
        current_ts=current_ts_2,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
        perform_merge_op=True,
    )
