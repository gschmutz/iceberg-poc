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

FILE_NAME = f"reports/{get_strategy_name().lower()}/scd2_test_type_array_ins.md"

load_ts_1 = datetime.strptime("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_1 = datetime.strptime("2026-01-02 00:00:00", "%Y-%m-%d %H:%M:%S")

load_ts_2 = datetime.strptime("2026-01-05 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_2 = datetime.strptime("2026-01-06 00:00:00", "%Y-%m-%d %H:%M:%S")

TEST_ENGINE = os.getenv("TEST_ENGINE", "SPARK").upper()

def test_step_1(ctx):
    logger.info(
        "-------------------------------- Test Step 1 --------------------------------"
    )

    create_raw_table(ctx, table_shape="array")
    create_scd2_table_for_test(ctx, table_shape="array")
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
                (1, ARRAY['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com'],  'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (2, ARRAY['Bob',   'Keller', 'Bern',   'bob.keller@example.com'],   'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (3, ARRAY['Clara', 'Schmid', 'Basel',  'clara.schmid@example.com'], 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
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
            ['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com'],   # user_info (ARRAY)
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "FCA31A8773CBA1D201E0FD46712E14B4730F9D768A1023645BFD80A6005A73D7",
        ),
        (
            2,
            ['Bob', 'Keller', 'Bern', 'bob.keller@example.com'],       # user_info (ARRAY)
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "546522DC8B793A74513486D16BE4C0A3A448BA8E65F6BE16E500D90FCB01DB66",
        ),
        (
            3,
            ['Clara', 'Schmid', 'Basel', 'clara.schmid@example.com'],  # user_info (ARRAY)
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "769F215ED61370AF86BE40AC7A29DFC3F067B55636B6918AD886C12619646091",
        ),
    ]

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=1,
        ins_stmt=insert_sql_1,
        table_shape="array",
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
                (1, ARRAY['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com'],  'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (2, ARRAY['Bob',   'Keller', 'Bern',   'bob.keller@example.com'],   'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (3, ARRAY['Clara', 'Schmid', 'Basel',  'clara.schmid@example.com'], 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (10, ARRAY['Kevin', 'Loosli', 'Bern',   'kevin.loosli@example.com'], 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}')
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
            ['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com'],   # user_info (no status)
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "FCA31A8773CBA1D201E0FD46712E14B4730F9D768A1023645BFD80A6005A73D7",
        ),
        (
            2,
            ['Bob', 'Keller', 'Bern', 'bob.keller@example.com'],       # user_info (no status)
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "546522DC8B793A74513486D16BE4C0A3A448BA8E65F6BE16E500D90FCB01DB66",
        ),
        (
            3,
            ['Clara', 'Schmid', 'Basel', 'clara.schmid@example.com'],  # user_info (no status)
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "769F215ED61370AF86BE40AC7A29DFC3F067B55636B6918AD886C12619646091",
        ),
        (
            10,
            ['Kevin', 'Loosli', 'Bern', 'kevin.loosli@example.com'],   # user_info (no status)
            load_ts_2,
            MAX_TS,
            True,
            True,
            current_ts_2,
            MAX_TS,
            "7F9D750B25D56FBC364C4D224E3999EDD183DC8F3AF153660450F165E001239C",
        ),
    ]

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=2,
        ins_stmt=insert_sql_2,
        table_shape="array",
        dp_ts=load_ts_2,
        current_ts=current_ts_2,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
        perform_merge_op=True,
    )
