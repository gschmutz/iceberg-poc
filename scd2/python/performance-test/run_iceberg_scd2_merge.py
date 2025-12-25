import sys
import os
import logging
import json
import uuid
import boto3
import random
import time
from datetime import date, timedelta
from faker import Faker
import pandas as pd
import numpy as np
from pyiceberg.catalog import load_catalog
from datetime import date, timedelta, datetime

import pyarrow as pa

from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
)
import s3fs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string, execute_with_metrics
import trino

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
ZONE = get_zone_name(upper=True)
# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
ENV = get_param('ENV', 'UAT', upper=True)

TRINO_USER = get_credential('TRINO_USER', 'trino')
TRINO_PASSWORD = get_credential('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'minio')
TRINO_USE_SSL = get_param('TRINO_USE_SSL', 'true').lower() in ('true', '1', 't')

# Connect to MinIO or AWS S3
ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')

S3_ADMIN_BUCKET = get_param('S3_ADMIN_BUCKET', 'admin-bucket')
S3_ADMIN_BUCKET = replace_vars_in_string(S3_ADMIN_BUCKET, { "zone": ZONE.upper(), "env": ENV.upper() } )
S3_ADMIN_BUCKET_PREFIX = get_param('S3_ADMIN_BUCKET_PREFIX', '')

INITIAL_PERSONS = 1_000_000   # scale here
UPDATE_RATE = 0.05
INSERT_RATE = 0.01
DELETE_RATE = 0.005

# Construct connection URLs
conn = trino.dbapi.connect(
    host=f"{TRINO_HOST}",
    port=int(TRINO_PORT),
    user=f"{TRINO_USER}",
    catalog=f"{TRINO_CATALOG}",
    schema="default",
    http_scheme="http",
)

# Create a session and S3 client
s3 = boto3.client('s3')

# Create S3 client configuration
s3_config = {"service_name": "s3"}
AWS_ACCESS_KEY = get_credential('AWS_ACCESS_KEY', None)
AWS_SECRET_ACCESS_KEY = get_credential('AWS_SECRET_ACCESS_KEY', None)

if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
    s3_config["aws_access_key_id"] = AWS_ACCESS_KEY
    s3_config["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
if ENDPOINT_URL:
    s3_config["endpoint_url"] = ENDPOINT_URL
    s3_config["verify"] = False  # Disable SSL verification for self-signed certificates

s3 = boto3.client(**s3_config)


def add_prefix(values, prefix):
    """add a prefix to each value."""
    if prefix:
        return [f"{prefix}.{v}" for v in values]
    else:
        return values

def format_values(values):
    """Join column names with an optional prefix."""
    return ", ".join(f"{v}" for v in values)

def cast_to_varchar(values):
    """Cast column names to VARCHAR."""
    return [f"CAST({value} AS VARCHAR)" for value in values]
    
def format_create_benchmark_table():
    ddl = f"""
        CREATE TABLE IF NOT EXISTS iceberg_hive."default".benchmark (
            benchmark_run_id VARCHAR,
            strategy VARCHAR,
            statement_name VARCHAR,
            query_id VARCHAR,
            elapsed_ms INT,
            cpu_ms INT,
            processed_rows BIGINT,
            processed_bytes BIGINT,
            success BOOLEAN,
            executed_at TIMESTAMP
        )
        WITH (
            location = 's3a://warehouse-bucket/warehouse/default/benchmark'
        )
    """
    return ddl

def format_create_dim_table(table_name: str, partioning_cols: list, sort_cols: list):

    partitioning_str = ", ".join([f"'{col}'" for col in partioning_cols]) if partioning_cols else ""
    sorted_by_str = ", ".join([f"'{col}'" for col in sort_cols]) if sort_cols else ""
    
    ddl = f"""
    CREATE TABLE IF NOT EXISTS iceberg_hive."default".{table_name} (
        surrogate_key VARCHAR,
        person_id VARCHAR,
        -- identity
        salutation VARCHAR,
        title VARCHAR,
        first_name VARCHAR,
        middle_name VARCHAR,
        last_name VARCHAR,
        suffix VARCHAR,
        gender VARCHAR,
        -- contact
        email VARCHAR,
        phone_mobile VARCHAR,
        phone_home VARCHAR,
        -- address
        street VARCHAR,
        house_number VARCHAR,
        postal_code VARCHAR,
        city VARCHAR,
        state VARCHAR,
        country VARCHAR,
        -- personal
        birth_date DATE,
        nationality VARCHAR,
        marital_status VARCHAR,
        number_of_children INT,
        -- employment
        employment_status VARCHAR,
        job_title VARCHAR,
        employer VARCHAR,
        annual_income DOUBLE,
        -- identifiers
        national_id VARCHAR,
        tax_id VARCHAR,
        -- metadata
        source_system VARCHAR,
        record_status VARCHAR,
        -- SCD2 metadata columns
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        is_current_version BOOLEAN,
        is_active BOOLEAN,
        source_loaded_at TIMESTAMP,
        created_at TIMESTAMP,
        replaced_at TIMESTAMP,
        -- Additional metadata
        change_type VARCHAR,
        record_hash VARCHAR
    )
    WITH (
        partitioning = ARRAY[{partitioning_str}],
        sorted_by = ARRAY[{sorted_by_str}],
        location = 's3a://warehouse-bucket/warehouse/default/{table_name}'
    )
    """
    return ddl

def format_cte(load_date: str, raw_table_name: str, dim_table_name: str, pk_col: str, val_columns: list):
    val_columns_str = format_values(val_columns)
    cast_val_columns_str = format_values(cast_to_varchar(val_columns))

    stmt = f"""
    WITH changed_records AS (
        SELECT 
            src.*,
            CASE 
                WHEN tgt.{pk_col} IS NULL THEN 'NEW'
                WHEN src.row_hash != tgt.row_hash THEN 'CHANGED'
                ELSE 'UNCHANGED'
            END AS change_classification
        FROM (
            SELECT *,
                to_hex(
                    sha256(
                        CAST(
                            concat_ws('||', ARRAY[{pk_col}, {cast_val_columns_str}, status])
                            AS VARBINARY
                        )
                    )
                ) AS row_hash
            FROM iceberg_hive.default.{raw_table_name}
            WHERE export_date = DATE '{load_date}'
        ) src
        LEFT JOIN (
            SELECT 
                surrogate_key,
                {pk_col},
                to_hex(
                    sha256(
                        CAST(
                            concat_ws('||', ARRAY[{pk_col}, {cast_val_columns_str}, CASE WHEN is_active THEN 'ACTIVE' ELSE 'INACTIVE' END])
                            AS VARBINARY
                        )
                    )
                ) AS row_hash,
                source_loaded_at,
                valid_from
            FROM iceberg_hive.default.{dim_table_name}
            WHERE is_current_version = TRUE
        ) tgt
        ON src.{pk_col} = tgt.{pk_col}
    ),
    records_to_process AS (
        SELECT *
        FROM changed_records
        WHERE change_classification IN ('NEW', 'CHANGED')
    ),
    prepared_source AS (
        -- Original records for updates
        SELECT
            surrogate_key,
            {pk_col} AS merge_key,
            {pk_col},
            {val_columns_str},
            export_date,
            status,
            'UPDATE_EXISTING' AS operation_type
        FROM records_to_process

        UNION ALL

        -- Duplicate records for inserts
        SELECT
            surrogate_key,
            NULL AS merge_key,
            {pk_col},
            {val_columns_str},
            export_date,
            status,
            'INSERT_NEW_VERSION' AS operation_type
        FROM records_to_process
        WHERE change_classification = 'CHANGED'
    )
    """
    return stmt

def format_view(load_date: str, raw_table_name: str, dim_table_name: str, pk_col: str, val_columns: list):
    cte = format_cte(load_date, raw_table_name, dim_table_name, pk_col, val_columns);
    stmt = f"""
    CREATE OR REPLACE VIEW minio.default.scd2_view AS
        {cte}
    SELECT *
    FROM prepared_source
    """
    return stmt

def format_merge(current_timestamp: str, raw_table_name: str, dim_table_name: str, pk_col: str, val_columns: list):
    prefixed_val_columns = add_prefix(val_columns, "source")

    val_columns_str = format_values(val_columns)
    source_val_columns_str = format_values(prefixed_val_columns)
    cast_source_val_columns_str = format_values(cast_to_varchar(prefixed_val_columns))    
    stmt = f"""


    MERGE INTO iceberg_hive.default.{dim_table_name} AS target
    USING minio.default.scd2_view AS source
    ON target.{pk_col} = source.merge_key
    AND target.is_current_version = TRUE

    WHEN MATCHED 
        AND source.operation_type = 'UPDATE_EXISTING'
        AND source.export_date > target.source_loaded_at
    THEN UPDATE SET
        valid_to = CAST(source.export_date AS TIMESTAMP) - INTERVAL '1' SECOND,
        is_current_version = FALSE,
        change_type = 'SUPERSEDED',
        created_at = TIMESTAMP '{current_timestamp}'

    WHEN NOT MATCHED
    THEN INSERT (
        surrogate_key,
        {pk_col},
        {val_columns_str},
        valid_from,
        valid_to,
        is_current_version,
        is_active,
        source_loaded_at,
        created_at,
        replaced_at,
        change_type,
        record_hash
    ) VALUES (
        source.surrogate_key,
        source.{pk_col},
        {source_val_columns_str},
        source.export_date,
        TIMESTAMP '9999-12-31 23:59:59',
        TRUE,
        CASE WHEN source.status = 'ACTIVE' THEN TRUE ELSE FALSE END,
        source.export_date,
        TIMESTAMP '{current_timestamp}',
        TIMESTAMP '9999-12-31 23:59:59',
        CASE 
            WHEN source.operation_type = 'UPDATE_EXISTING' THEN 'NEW'
            ELSE 'SUPERSEDED_BY'
        END,
        to_hex(
            sha256(
                CAST(
                    concat_ws('||', ARRAY[CAST(source.{pk_col} AS VARCHAR), {cast_source_val_columns_str}, status])
                    AS VARBINARY
                )
            )
        )
    )
    """

    return stmt

def insert_benchmark_metrics(cursor, benchmark_run_id: str, strategy: str, statement_name: str, result: dict):
    INSERT_SQL = """
        INSERT INTO iceberg_hive.default.benchmark (benchmark_run_id, strategy, statement_name, query_id, elapsed_ms, cpu_ms, processed_rows, processed_bytes, success, executed_at)
        VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """

    cursor.execute(
        INSERT_SQL,
        (
            benchmark_run_id,
            strategy,
            statement_name,
            result["query_id"],
            result["elapsed_ms"],
            result["cpu_ms"],
            result["processed_rows"],
            result["processed_bytes"],
            result["success"],
            result["executed_at"],
        ),
    )
    cursor.fetchall()

def run_dim_create_table(tshirt: str, case_id: int, partition_cols: list = None, sort_cols: list = None):

    table_name = f"dim_person_{case_id}_{tshirt}"
    drop_table_stmt = f"""DROP TABLE IF EXISTS iceberg_hive."default".{table_name}"""
    print(drop_table_stmt)
    execute_with_metrics(conn.cursor(), drop_table_stmt)

    create_table_stmt = format_create_dim_table(table_name, partition_cols, sort_cols)
    print(create_table_stmt)

    execute_with_metrics(conn.cursor(), create_table_stmt)
    logger.info(f"Dimension table for thsirt {tshirt} and test-case {case_id} created successfully.")

def run_benchmark_create_table():

    drop_table_stmt = f"""DROP TABLE IF EXISTS iceberg_hive."default".benchmark"""
    print(drop_table_stmt)
    execute_with_metrics(conn.cursor(), drop_table_stmt)

    create_table_stmt = format_create_benchmark_table()

    print(create_table_stmt)

    execute_with_metrics(conn.cursor(), create_table_stmt)
    logger.info(f"Benchmark table created successfully.")

def run_dim_update(tshirt: str, case_id: int, case_description: str, day_number: int, load_date: str):

    columns = [
        "salutation", "title", "first_name", "middle_name", "last_name", "suffix",
        "gender", "email", "phone_mobile", "phone_home", "street", "house_number",
        "postal_code", "city", "state", "country", "birth_date", "nationality",
        "marital_status", "number_of_children", "employment_status", "job_title",
        "employer", "annual_income", "national_id", "tax_id"
    ]

    dim_table_name = f"dim_person_{case_id}_{tshirt}"

    view_stmt = format_view(
        load_date=load_date,
        raw_table_name=f"raw_person_{tshirt}",
        dim_table_name=dim_table_name,
        pk_col="person_id",
        val_columns=columns
    )

    merge_stmt = format_merge(
        current_timestamp="2025-12-18 12:00:00",
        raw_table_name=f"raw_person_{tshirt}",
        dim_table_name=dim_table_name,
        pk_col="person_id",
        val_columns=columns
    )
    
    execute_with_metrics(conn.cursor(), view_stmt)

    result = execute_with_metrics(conn.cursor(), merge_stmt)
    print (f"Executed test-case {case_id} for day {day_number} for load date {load_date} in {result["elapsed_ms"]} ms")

    insert_benchmark_metrics(cursor=conn.cursor(), benchmark_run_id=f"test-run-{case_id}-{day_number}", strategy=f"SCD2_MERGE_{case_id}_{tshirt}", statement_name=case_description, result=result)

    logger.info(f"Merge statement for {load_date} {result}")
        

def run_merge_all(tshirt: str, case_id: int, case_description: str, partition_cols: list = None, sort_cols: list = None):

    run_dim_create_table(tshirt=tshirt, case_id=case_id, partition_cols=partition_cols, sort_cols=sort_cols)

    start_date = date(2024, 1, 1)
    DAYS = 30
    for d in range(DAYS):
        load_date = start_date + timedelta(days=d)
        print (load_date)
        run_dim_update(tshirt=tshirt, case_id=case_id, case_description=case_description, day_number=d,load_date=load_date.strftime("%Y-%m-%d"))


def run_test_cases(tshirt: str):
    
    #run_benchmark_create_table()

    # Load and execute all test cases
    with open('test-cases.json', 'r') as f:
        test_data = json.load(f)

    for test_case in test_data['test_cases']:
        logger.info(f"Running test case {test_case['case_id']}: {test_case['description']}")
        
        run_merge_all(tshirt=tshirt, case_id=test_case['case_id'], case_description=test_case['description'], partition_cols=test_case.get('partition_cols'), sort_cols=test_case.get('sort_cols'))
 
run_test_cases(tshirt="XL")
