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

FILE_NAME = f"reports/{get_strategy_name().lower()}/scd2_test_type_map_ins.md"

load_ts_1 = datetime.strptime("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_1 = datetime.strptime("2026-01-02 00:00:00", "%Y-%m-%d %H:%M:%S")

load_ts_2 = datetime.strptime("2026-01-05 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_2 = datetime.strptime("2026-01-06 00:00:00", "%Y-%m-%d %H:%M:%S")

TEST_ENGINE = os.getenv("TEST_ENGINE", "SPARK").upper()

def test_step_1(ctx):
    logger.info(
        "-------------------------------- Test Step 1 --------------------------------"
    )

    create_raw_table(ctx, table_shape="map")
    create_scd2_table_for_test(ctx, table_shape="map")
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
                (1, MAP(ARRAY['first_name', 'last_name', 'city', 'email'], ARRAY['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com']), 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (2, MAP(ARRAY['first_name', 'last_name', 'city', 'email'], ARRAY['Bob', 'Keller', 'Bern', 'bob.keller@example.com']), 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (3, MAP(ARRAY['first_name', 'last_name', 'city', 'email'], ARRAY['Clara', 'Schmid', 'Basel', 'clara.schmid@example.com']), 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
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
            "F3EB9083D97A374919FF0C4FC913D263F9A437B1398888EA2D902A35D71DFF18",
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
            "8E22796C7A86F5A51D957BFC1CB1415692F924C4B9FE7497039C06F5BA26EB63",
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
            "D52C3A1221C144C1E50AECD6A257AA02EA146B298C8A4F1C771FCBE552E504E8",
        ),
    ]

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=1,
        ins_stmt=insert_sql_1,
        table_shape="map",
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
                (1, MAP(ARRAY['first_name', 'last_name', 'city', 'email'], ARRAY['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com']), 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (2, MAP(ARRAY['first_name', 'last_name', 'city', 'email'], ARRAY['Bob', 'Keller', 'Bern', 'bob.keller@example.com']), 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (3, MAP(ARRAY['first_name', 'last_name', 'city', 'email'], ARRAY['Clara', 'Schmid', 'Basel', 'clara.schmid@example.com']), 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (10, MAP(ARRAY['first_name', 'last_name', 'city', 'email'], ARRAY['Kevin', 'Loosli', 'Bern', 'kevin.loosli@example.com']), 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}')
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
            "F3EB9083D97A374919FF0C4FC913D263F9A437B1398888EA2D902A35D71DFF18",
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
            "8E22796C7A86F5A51D957BFC1CB1415692F924C4B9FE7497039C06F5BA26EB63",
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
            "D52C3A1221C144C1E50AECD6A257AA02EA146B298C8A4F1C771FCBE552E504E8",
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
            "D7A624F643B937EE218EB4F1ECA9AB207C61063D4AF96FBB22FFADB54292834D",
        ),
    ]

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=2,
        ins_stmt=insert_sql_2,
        table_shape="map",
        dp_ts=load_ts_2,
        current_ts=current_ts_2,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
        perform_merge_op=True,
    )
