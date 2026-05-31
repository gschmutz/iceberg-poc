import logging
import os
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from datetime import date, datetime

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
from constants import MAX_TS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_NAME = f"reports/{get_strategy_name().lower()}/scd2_test_type_various_ins.md"

load_ts_1 = datetime.strptime("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_1 = datetime.strptime("2026-01-02 00:00:00", "%Y-%m-%d %H:%M:%S")

load_ts_2 = datetime.strptime("2026-01-05 00:00:00", "%Y-%m-%d %H:%M:%S")
current_ts_2 = datetime.strptime("2026-01-06 00:00:00", "%Y-%m-%d %H:%M:%S")


def test_step_1(ctx):
    logger.info(
        "-------------------------------- Test Step 1 --------------------------------"
    )

    create_raw_table(ctx, table_shape="various")
    create_scd2_table_for_test(ctx, table_shape="various")
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
                (1, TINYINT '1', SMALLINT '100', 123456, BIGINT '9876543210', REAL '3.14', DOUBLE '3.141592653589793', DECIMAL '12345.6789', VARCHAR 'hello world', VARCHAR 'limited', CHAR 'fixed     ', TRUE,  DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00.000000', TIMESTAMP '2024-01-15 10:30:00.000000 UTC', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (2, TINYINT '2', SMALLINT '200', 234567, BIGINT '8765432109', REAL '2.71', DOUBLE '2.718281828459045', DECIMAL '23456.789', VARCHAR 'world hello', VARCHAR 'another',  CHAR 'fixed     ', FALSE, DATE '2024-02-20', TIMESTAMP '2024-02-20 11:45:00.000000', TIMESTAMP '2024-02-20 11:45:00.000000 UTC', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}'),
                (3, TINYINT '3', SMALLINT '300', 345678, BIGINT '7654321098', REAL '1.41', DOUBLE '1.414213562373095', DECIMAL '34567.8901', VARCHAR 'foo bar',     VARCHAR 'short',    CHAR 'fixed     ', TRUE,  DATE '2024-03-25', TIMESTAMP '2024-03-25 12:00:00.000000', TIMESTAMP '2024-03-25 12:00:00.000000 UTC', 'ACTIVE', TIMESTAMP '{load_ts_1}', TIMESTAMP '{load_ts_1}')
        ) AS t (
            id,
            col_tinyint,
            col_smallint,
            col_int,
            col_bigint,
            col_real,
            col_double,
            col_decimal,
            col_varchar,
            col_varchar_n,
            col_char,
            col_boolean,
            col_date,
            col_timestamp,
            col_timestamp_tz,
            active,
            dp_ts_from,
            dp_loaded_at
        )
    """

    expected = [
        (
            1,
            1,           # col_tinyint
            100,         # col_smallint
            123456,      # col_int
            9876543210,  # col_bigint
            3.14,        # col_real
            3.141592653589793,  # col_double
            Decimal("12345.6789"),  # col_decimal
            "hello world",  # col_varchar
            "limited",      # col_varchar_n
            "fixed     ",   # col_char
            True,           # col_boolean
            date(2024, 1, 15),                          # col_date
            datetime(2024, 1, 15, 10, 30, 0),           # col_timestamp
            datetime(2024, 1, 15, 10, 30, 0),           # col_timestamp_tz
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "55EFCF5C8C7685252FDFB7E4A523E4EF4EC953164868199B8E8EA7F5D1363C2C",
        ),
        (
            2,
            2,           # col_tinyint
            200,         # col_smallint
            234567,      # col_int
            8765432109,  # col_bigint
            2.71,        # col_real
            2.718281828459045,  # col_double
            Decimal("23456.789"),  # col_decimal
            "world hello",  # col_varchar
            "another",      # col_varchar_n
            "fixed     ",   # col_char
            False,          # col_boolean
            date(2024, 2, 20),                          # col_date
            datetime(2024, 2, 20, 11, 45, 0),           # col_timestamp
            datetime(2024, 2, 20, 11, 45, 0),           # col_timestamp_tz
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "BFBCD5B72A3810ED483A792ED0DF213A4F920E50971C8A2833E1983FA53CE926",
        ),
        (
            3,
            3,           # col_tinyint
            300,         # col_smallint
            345678,      # col_int
            7654321098,  # col_bigint
            1.41,        # col_real
            1.414213562373095,  # col_double
            Decimal("34567.8901"),  # col_decimal
            "foo bar",      # col_varchar
            "short",        # col_varchar_n
            "fixed     ",   # col_char
            True,           # col_boolean
            date(2024, 3, 25),                          # col_date
            datetime(2024, 3, 25, 12, 0, 0),            # col_timestamp
            datetime(2024, 3, 25, 12, 0, 0),            # col_timestamp_tz
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "2A80AE5B248E83C5939210F6AA16C840C36ED620C2F3FDAD680C442120AE7AD7",
        ),
    ]

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=1,
        ins_stmt=insert_sql_1,
        table_shape="various",
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
                (1, TINYINT '1', SMALLINT '100', 123456, BIGINT '9876543210', REAL '3.14', DOUBLE '3.141592653589793', DECIMAL '12345.6789', VARCHAR 'hello world', VARCHAR 'limited', CHAR 'fixed     ', TRUE,  DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00.000000', TIMESTAMP '2024-01-15 10:30:00.000000 UTC', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (2, TINYINT '2', SMALLINT '200', 234567, BIGINT '8765432109', REAL '2.71', DOUBLE '2.718281828459045', DECIMAL '23456.789', VARCHAR 'world hello', VARCHAR 'another',  CHAR 'fixed     ', FALSE, DATE '2024-02-20', TIMESTAMP '2024-02-20 11:45:00.000000', TIMESTAMP '2024-02-20 11:45:00.000000 UTC', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (3, TINYINT '3', SMALLINT '300', 345678, BIGINT '7654321098', REAL '1.41', DOUBLE '1.414213562373095', DECIMAL '34567.8901', VARCHAR 'foo bar',     VARCHAR 'short',    CHAR 'fixed     ', TRUE,  DATE '2024-03-25', TIMESTAMP '2024-03-25 12:00:00.000000', TIMESTAMP '2024-03-25 12:00:00.000000 UTC', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}'),
                (10, TINYINT '10', SMALLINT '1000', 1234567, BIGINT '98765432100', REAL '0.99', DOUBLE '0.999999999999999', DECIMAL '123456.7890', VARCHAR 'new entity', VARCHAR 'new', CHAR 'fixed     ', FALSE, DATE '2024-04-30', TIMESTAMP '2024-04-30 14:00:00.000000', TIMESTAMP '2024-04-30 14:00:00.000000 UTC', 'ACTIVE', TIMESTAMP '{load_ts_2}', TIMESTAMP '{load_ts_2}')
        ) AS t (
            id,
            col_tinyint,
            col_smallint,
            col_int,
            col_bigint,
            col_real,
            col_double,
            col_decimal,
            col_varchar,
            col_varchar_n,
            col_char,
            col_boolean,
            col_date,
            col_timestamp,
            col_timestamp_tz,
            active,
            dp_ts_from,
            dp_loaded_at
        )
    """

    expected = [
        (
            1,
            1,           # col_tinyint
            100,         # col_smallint
            123456,      # col_int
            9876543210,  # col_bigint
            3.14,        # col_real
            3.141592653589793,  # col_double
            Decimal("12345.6789"),  # col_decimal
            "hello world",  # col_varchar
            "limited",      # col_varchar_n
            "fixed     ",   # col_char
            True,           # col_boolean
            date(2024, 1, 15),                          # col_date
            datetime(2024, 1, 15, 10, 30, 0),           # col_timestamp
            datetime(2024, 1, 15, 10, 30, 0),           # col_timestamp_tz
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "55EFCF5C8C7685252FDFB7E4A523E4EF4EC953164868199B8E8EA7F5D1363C2C",
        ),
        (
            2,
            2,           # col_tinyint
            200,         # col_smallint
            234567,      # col_int
            8765432109,  # col_bigint
            2.71,        # col_real
            2.718281828459045,  # col_double
            Decimal("23456.789"),  # col_decimal
            "world hello",  # col_varchar
            "another",      # col_varchar_n
            "fixed     ",   # col_char
            False,          # col_boolean
            date(2024, 2, 20),                          # col_date
            datetime(2024, 2, 20, 11, 45, 0),           # col_timestamp
            datetime(2024, 2, 20, 11, 45, 0),           # col_timestamp_tz
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "BFBCD5B72A3810ED483A792ED0DF213A4F920E50971C8A2833E1983FA53CE926",
        ),
        (
            3,
            3,           # col_tinyint
            300,         # col_smallint
            345678,      # col_int
            7654321098,  # col_bigint
            1.41,        # col_real
            1.414213562373095,  # col_double
            Decimal("34567.8901"),  # col_decimal
            "foo bar",      # col_varchar
            "short",        # col_varchar_n
            "fixed     ",   # col_char
            True,           # col_boolean
            date(2024, 3, 25),                          # col_date
            datetime(2024, 3, 25, 12, 0, 0),            # col_timestamp
            datetime(2024, 3, 25, 12, 0, 0),            # col_timestamp_tz
            load_ts_1,
            MAX_TS,
            True,
            True,
            current_ts_1,
            MAX_TS,
            "2A80AE5B248E83C5939210F6AA16C840C36ED620C2F3FDAD680C442120AE7AD7",
        ),
        (
            10,
            10,           # col_tinyint
            1000,         # col_smallint
            1234567,      # col_int
            98765432100,  # col_bigint
            0.99,        # col_real
            0.999999999999999,  # col_double
            Decimal("123456.7890"),  # col_decimal
            "new entity",      # col_varchar
            "new",        # col_varchar_n
            "fixed     ",   # col_char
            False,           # col_boolean
            date(2024, 4, 30),                          # col_date
            datetime(2024, 4, 30, 14, 0, 0),            # col_timestamp
            datetime(2024, 4, 30, 14, 0, 0),            # col_timestamp_tz
            load_ts_2,
            MAX_TS,
            True,
            True,
            current_ts_2,
            MAX_TS,
            "87B6B45C5FA936554CF1312EB81DECE3E5C8D037FAF36B9DE4FB72071A7319A1",
        ),        
    ]

    # run test
    scd2_merge_as_test(
        ctx,
        test_step=2,
        ins_stmt=insert_sql_2,
        table_shape="various",        
        dp_ts=load_ts_2,
        current_ts=current_ts_2,
        expected=expected,
        output_file_name=FILE_NAME,
        test_description=test_description,
        perform_merge_op=True,
    )
