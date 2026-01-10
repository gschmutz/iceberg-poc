import sys
import os
import logging
import json
import uuid
import boto3
import random
import time
import argparse
from datetime import date, timedelta
from faker import Faker
import pandas as pd
import numpy as np
from pyiceberg.catalog import load_catalog
from datetime import date, timedelta, datetime

import pyarrow as pa

import trino
from trino.auth import BasicAuthentication

from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string, execute_with_metrics
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
from scd2 import merge_into_dim_table, create_dim_table 

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# size of benchmark
TSHIRT_SIZE = get_param('TSHIRT_SIZE', 'xl').lower()
NOF_DAYS = int(get_param('NOF_DAYS', '30'))
 
ZONE = get_zone_name(upper=True)
# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
ENV = get_param('ENV', 'UAT', upper=True)

TRINO_USER = get_credential('TRINO_USER', 'trino')
TRINO_PASSWORD = get_credential('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'minio')
TRINO_SCHEMA = get_param('TRINO_SCHEMA', 'default')
TRINO_USE_SSL = get_param('TRINO_USE_SSL', 'true').lower() in ('true', '1', 't')

# Connect to MinIO or AWS S3
S3_ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')

S3_WAREHOUSE_BUCKET = get_param('S3_WAREHOUSE_BUCKET', 'warehouse-bucket')
S3_WAREHOUSE_BUCKET = replace_vars_in_string(S3_WAREHOUSE_BUCKET, { "zone": "", "env": "" } )
S3_WAREHOUSE_PREFIX = get_param('S3_WAREHOUSE_PREFIX', 'iceberg-poc')
S3_WAREHOUSE_PREFIX = replace_vars_in_string(S3_WAREHOUSE_PREFIX, { "zone": "", "env": "" } )
S3_UPLOAD_BUCKET = get_param('S3_UPLOAD_BUCKET', 'upload-bucket')
S3_UPLOAD_BUCKET = replace_vars_in_string(S3_UPLOAD_BUCKET, { "zone": "", "env": "" } )
S3_UPLOAD_PREFIX = get_param('S3_UPLOAD_PREFIX', 'iceberg-poc')
S3_UPLOAD_PREFIX = replace_vars_in_string(S3_UPLOAD_PREFIX, { "zone": "", "env": "" } )
AWS_ACCESS_KEY = get_credential('AWS_ACCESS_KEY', None)
AWS_SECRET_ACCESS_KEY = get_credential('AWS_SECRET_ACCESS_KEY', None)
DOWNLOAD_TEST_CASES_FROM_S3 = get_param('DOWNLOAD_TEST_CASES_FROM_S3', 'false').lower() in ('true', '1', 't')

INITIAL_PERSONS = 1_000_000   # scale here
UPDATE_RATE = 0.05
INSERT_RATE = 0.01
DELETE_RATE = 0.005

# Create a session and S3 client
s3 = boto3.client('s3')

# Create S3 client configuration
s3_config = {"service_name": "s3"}

if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
    s3_config["aws_access_key_id"] = AWS_ACCESS_KEY
    s3_config["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
if S3_ENDPOINT_URL:
    s3_config["endpoint_url"] = S3_ENDPOINT_URL
    s3_config["verify"] = False  # Disable SSL verification for self-signed certificates

s3 = boto3.client(**s3_config)

cols_with_type = [
    "salutation VARCHAR",
    "title VARCHAR",
    "first_name VARCHAR",
    "middle_name VARCHAR",
    "last_name VARCHAR",
    "suffix VARCHAR",
    "gender VARCHAR",
    "email VARCHAR",
    "phone_mobile VARCHAR",
    "phone_home VARCHAR",
    "street VARCHAR",
    "house_number VARCHAR",
    "postal_code VARCHAR",
    "city VARCHAR",
    "state VARCHAR",
    "country VARCHAR",
    "birth_date DATE",
    "nationality VARCHAR",
    "marital_status VARCHAR",
    "number_of_children INT",
    "employment_status VARCHAR",
    "job_title VARCHAR",
    "employer VARCHAR",
    "annual_income DOUBLE",
    "national_id VARCHAR",
    "tax_id VARCHAR",
]

def get_trino_connection():

    if TRINO_USE_SSL:
        http_scheme = "https"
    else:
        http_scheme = "http"

    # Construct connection URLs
    conn = trino.dbapi.connect(
        host=f"{TRINO_HOST}",
        port=int(TRINO_PORT),
        user=f"{TRINO_USER}",
        catalog=f"{TRINO_CATALOG}",
        schema=f"{TRINO_SCHEMA}",
        http_scheme=http_scheme,
        auth=BasicAuthentication(
            TRINO_USER,
            TRINO_PASSWORD
        ) if TRINO_PASSWORD else None,
        verify=False  # Disable SSL verification for self-signed certificates,
    )

    return conn

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
        CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.benchmark (
            run_id VARCHAR,
            case_id VARCHAR,
            day_number INT,
            tshirt_size VARCHAR,
            dim_table_name VARCHAR,
            statement_key VARCHAR,
            statement_name VARCHAR,
            query_id VARCHAR,
            elapsed_ms INT,
            cpu_ms INT,
            processed_rows BIGINT,
            processed_bytes BIGINT,
            success BOOLEAN,
            error_message VARCHAR,
            executed_at TIMESTAMP,
            iceberg_snapshot_id BIGINT,
            iceberg_nof_files BIGINT,
            iceberg_status_list ARRAY<VARCHAR>,
            iceberg_file_list ARRAY<VARCHAR>
        )
        WITH (
            location = 's3a://{S3_WAREHOUSE_BUCKET}/{S3_WAREHOUSE_PREFIX}/{TRINO_SCHEMA}/benchmark'
        )
    """
    return ddl


def create_benchmark_table(drop_it_first: bool):
    conn = get_trino_connection()

    if drop_it_first:
        drop_table_stmt = f"""DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.benchmark"""
        logger.info(f"Benchmark table dropped successfully.")
        execute_with_metrics(conn.cursor(), drop_table_stmt)

    create_table_stmt = format_create_benchmark_table()

    print(create_table_stmt)

    cursor = conn.cursor()
    cursor.execute(create_table_stmt)

    logger.info(f"Benchmark table created successfully.")

def insert_benchmark_metrics(cursor, run_id: str, case_id: str, day_number: int, tshirt_size: str, dim_table_name: str, statement_key: str, statement_name: str, result: dict, iceberg_metadata: list = []):

    INSERT_SQL = f"""
        INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.benchmark (run_id, case_id, day_number, tshirt_size, dim_table_name, statement_key, statement_name, query_id, elapsed_ms, cpu_ms, processed_rows, processed_bytes, success, error_message, executed_at, iceberg_snapshot_id, iceberg_nof_files, iceberg_status_list, iceberg_file_list)
        VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """

    cursor.execute(
        INSERT_SQL,
        (
            run_id, 
            case_id,
            day_number,
            tshirt_size,
            dim_table_name,
            statement_key,
            statement_name,
            result["query_id"],
            result["elapsed_ms"],
            result["cpu_ms"],
            result["processed_rows"],
            result["processed_bytes"],
            result["success"],
            result["error"],
            result["executed_at"],
            iceberg_metadata[0] if iceberg_metadata else None,
            iceberg_metadata[1] if iceberg_metadata else None,
            iceberg_metadata[2] if iceberg_metadata else None,
            iceberg_metadata[3] if iceberg_metadata else None
        ),
    )
    cursor.fetchall()

def run_analyze_table(tshirt: str, case_id: int):
    conn = get_trino_connection()

    table_name = f"dim_person_{case_id}_{tshirt}"
    optimize_stmt = f"""
                    ANALYZE {table_name}
                    """
    print(optimize_stmt)
    result = execute_with_metrics(conn.cursor(), optimize_stmt)
    print (result)
    logger.info(f"Analyze table for thsirt {tshirt} and test-case {case_id} executed successfully.")

def run_optimize_table(table_name: str):
    conn = get_trino_connection()

    optimize_stmt = f"""
                    ALTER TABLE {table_name}
                    EXECUTE optimize (file_size_threshold => '256MB')
                    """
    print(optimize_stmt)
    result = execute_with_metrics(conn.cursor(), optimize_stmt)
    print (result)
    logger.info(f"Optimize table for {table_name} executed successfully.")

        
def run_merge_all(tshirt: str, run_id: str, case_id: int, case_description: str, partition_cols: list = None, sort_cols: list = None):
    table_name = "person"
    dim_table_name = f"dim_{table_name}_{case_id}_{tshirt}"
    raw_table_name = f"raw_{table_name}_{tshirt}"
    
    conn = get_trino_connection()
    create_dim_table(conn, TRINO_CATALOG, TRINO_SCHEMA, f"dim_{table_name}_{case_id}_{tshirt}", s3_warehouse_bucket=S3_WAREHOUSE_BUCKET, s3_warehouse_prefix=S3_WAREHOUSE_PREFIX, pk_col_with_type=f"{table_name}_id VARCHAR", cols_with_type=cols_with_type, partition_cols=partition_cols, sort_cols=sort_cols)

    start_ts = datetime(2024, 1, 1, 0, 0, 0)
    for day in range(NOF_DAYS):
        load_date = start_ts + timedelta(days=day)
        print (load_date)

        result, iceberg_metadata = merge_into_dim_table(
            conn=conn,
            trino_catalog=TRINO_CATALOG,
            trino_schema=TRINO_SCHEMA,
            raw_table_name=raw_table_name,
            dim_table_name=dim_table_name,
            scd2_view_name="view_person_scd2",
            load_ts=load_date.strftime("%Y-%m-%d"),
            load_ts_col="export_date",
            pk_col="person_id",
            cols_with_type=cols_with_type,
            current_timestamp=(start_ts + timedelta(days=day)).strftime("%Y-%m-%d %H:%M:%S"),
        )

        insert_benchmark_metrics(cursor=conn.cursor(), run_id=run_id, case_id=f"{case_id}", day_number=day, tshirt_size=tshirt, dim_table_name=dim_table_name, statement_key=f"SCD2_MERGE_{case_id}_{tshirt}", statement_name=case_description, result=result, iceberg_metadata=iceberg_metadata)

        # run optimize every 5 days
        if day > 0 and day % 5 == 0:
            run_optimize_table(table_name=dim_table_name)

    # run optimize for dim table and benchmark at the end as well
    run_optimize_table(table_name=dim_table_name)
    run_optimize_table(table_name="benchmark")

def run_select_one(tshirt: str, run_id: str, case_id: int, person_id: str, restrict_active_expression: str = ""):
    conn = get_trino_connection()
    
    query = f"""
        SELECT *
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.dim_person_{case_id}_{tshirt}
        WHERE person_id = '{person_id}'
    """
    result = execute_with_metrics(conn.cursor(), query)

    insert_benchmark_metrics(cursor=conn.cursor(), run_id=run_id, case_id=f"{case_id}", day_number=0, tshirt_size=tshirt, dim_table_name=f"dim_person_{case_id}_{tshirt}", statement_key=f"SCD2_SELECT_ONE_{case_id}_{tshirt}", statement_name="select one person by PK", result=result)

def run_select_count_active(tshirt: str, run_id: str, case_id: int, restrict_active_expression: str = ""):
    conn = get_trino_connection()

    query = f"""
        SELECT sum(length(cast(first_name AS varchar))) AS length_sum, COUNT(*) AS person_count
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.dim_person_{case_id}_{tshirt}
        WHERE {restrict_active_expression}
    """
    result = execute_with_metrics(conn.cursor(), query)

    insert_benchmark_metrics(cursor=conn.cursor(), run_id=run_id, case_id=f"{case_id}", day_number=0, tshirt_size=tshirt, dim_table_name=f"dim_person_{case_id}_{tshirt}", statement_key=f"SCD2_SELECT_COUNT_ACTIVE_{case_id}_{tshirt}", statement_name="count all active persons", result=result) 

def run_select_count_latest(tshirt: str, run_id: str, case_id: int, restrict_active_expression: str = ""):
    conn = get_trino_connection()

    query = f"""
        SELECT sum(length(cast(first_name AS varchar))) AS length_sum, COUNT(*) AS person_count
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.dim_person_{case_id}_{tshirt}
        WHERE dp_is_latest = TRUE
    """
    result = execute_with_metrics(conn.cursor(), query)

    insert_benchmark_metrics(cursor=conn.cursor(), run_id=run_id, case_id=f"{case_id}", day_number=0, tshirt_size=tshirt, dim_table_name=f"dim_person_{case_id}_{tshirt}", statement_key=f"SCD2_SELECT_COUNT_LATEST_{case_id}_{tshirt}", statement_name="count all active persons", result=result) 

def run_select_count_by_gender(tshirt: str, run_id: str, case_id: int, restrict_active_expression: str = ""):
    conn = get_trino_connection()

    query = f"""
        SELECT gender, array_agg(person_id) AS persons, COUNT(person_id) AS gender_count
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.dim_person_{case_id}_{tshirt}
        WHERE {restrict_active_expression}
        GROUP BY gender
    """
    result = execute_with_metrics(conn.cursor(), query)

    insert_benchmark_metrics(cursor=conn.cursor(), run_id=run_id, case_id=f"{case_id}", day_number=0, tshirt_size=tshirt, dim_table_name=f"dim_person_{case_id}_{tshirt}", statement_key=f"SCD2_SELECT_COUNT_BY_GENDER_{case_id}_{tshirt}", statement_name="count by gender for all active persons", result=result) 

def run_select_count_by_gender_latest(tshirt: str, run_id: str, case_id: int, restrict_active_expression: str = ""):
    conn = get_trino_connection()

    query = f"""
        SELECT gender, array_agg(person_id) AS persons, COUNT(person_id) AS gender_count
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.dim_person_{case_id}_{tshirt}
        WHERE {restrict_active_expression}
        AND dp_is_latest = TRUE
        GROUP BY gender
    """
    result = execute_with_metrics(conn.cursor(), query)

    insert_benchmark_metrics(cursor=conn.cursor(), run_id=run_id, case_id=f"{case_id}", day_number=0, tshirt_size=tshirt, dim_table_name=f"dim_person_{case_id}_{tshirt}", statement_key=f"SCD2_SELECT_COUNT_BY_GENDER_FOR_LATEST_{case_id}_{tshirt}", statement_name="count by gender for all latest persons", result=result) 

def run_select_nof_person_in_ch_at_5th_of_jan(tshirt: str, run_id: str, case_id: int, restrict_active_expression: str = ""):
    conn = get_trino_connection()

    query = f"""
        SELECT COUNT(*) AS person_in_ch, array_agg(person_id) AS persons
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.dim_person_{case_id}_{tshirt}
        where cast('2024-01-05' as date) between dp_valid_from and dp_valid_to
        AND {restrict_active_expression} 
        AND dp_is_active = TRUE
        AND country = 'CH'
    """
    result = execute_with_metrics(conn.cursor(), query)

    insert_benchmark_metrics(cursor=conn.cursor(), run_id=run_id, case_id=f"{case_id}", day_number=0, tshirt_size=tshirt, dim_table_name=f"dim_person_{case_id}_{tshirt}", statement_key=f"SCD2_SELECT_NOF_PERSONS_IN_CH_ON_DAY_{case_id}_{tshirt}", statement_name="count all persons in CH which where active on 5 jan", result=result) 

def run_test_cases(number_of_runs: int, run_for_test_cases: list, run_select_only: bool = False, drop_benchmark_table_first: bool = False):

    tshirt = TSHIRT_SIZE.lower()

    logger.info(f"Running test-cases for tshirt size {tshirt} for {number_of_runs} runs")

    person_id = "1349659"  # known person_id to select at the end
    
    create_benchmark_table(drop_benchmark_table_first)

    local_file = f"test-cases.json"
    
    if DOWNLOAD_TEST_CASES_FROM_S3:
        # Download the initial dataset from S3
        s3_key = f"{S3_UPLOAD_PREFIX}/initial-dataset/test-cases.json"
        logger.info(f"Downloading s3://{S3_UPLOAD_BUCKET}/{s3_key} to {local_file}")
        s3.download_file(S3_UPLOAD_BUCKET, s3_key, local_file)
        logger.info(f"Successfully downloaded {local_file} from S3")

    # Load and execute all test cases
    with open(local_file, 'r') as f:
        test_data = json.load(f)

    for _ in range(number_of_runs):
        run_id: str = str(uuid.uuid4())

        for test_case in test_data['test_cases']:
            if not test_case.get('active', True):
                continue
            if run_for_test_cases and test_case['case_id'] not in run_for_test_cases:
                continue
            logger.info(f"Running test case {test_case['case_id']}: {test_case['description']}")

            if not run_select_only:            
                run_merge_all(tshirt=tshirt, run_id=run_id, case_id=test_case['case_id'], case_description=test_case['description'], partition_cols=test_case.get('partition_cols'), sort_cols=test_case.get('sort_cols'))
    
            # At the end let's perform some selects to benchmark read performance
            for _ in range(number_of_runs*2):
                run_select_one(tshirt=tshirt, run_id=run_id, case_id=test_case['case_id'], person_id=person_id, restrict_active_expression=test_case.get('restrict_active_expression'))
                run_select_count_active(tshirt=tshirt, run_id=run_id, case_id=test_case['case_id'], restrict_active_expression=test_case.get('restrict_active_expression'))
                run_select_count_latest(tshirt=tshirt, run_id=run_id, case_id=test_case['case_id'], restrict_active_expression=test_case.get('restrict_active_expression'))
                run_select_count_by_gender(tshirt=tshirt, run_id=run_id, case_id=test_case['case_id'], restrict_active_expression=test_case.get('restrict_active_expression'))
                run_select_count_by_gender_latest(tshirt=tshirt, run_id=run_id, case_id=test_case['case_id'], restrict_active_expression=test_case.get('restrict_active_expression'))
                run_select_nof_person_in_ch_at_5th_of_jan(tshirt=tshirt, run_id=run_id, case_id=test_case['case_id'], restrict_active_expression=test_case.get('restrict_active_expression'))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", help="Command to run")
    parser.add_argument("--number_of_runs", default=1, type=int)
    parser.add_argument("--run_for_test_cases", default=[], type=list)
    parser.add_argument("--run_select_only", default=False, type=bool)
    parser.add_argument("--drop_benchmark_table_first", default=False, type=bool)

    args = parser.parse_args()

    if args.command == "run_test_cases":
        run_test_cases(args.number_of_runs, args.run_for_test_cases, run_select_only=args.run_select_only, drop_benchmark_table_first=args.drop_benchmark_table_first)
    else:
        logger.error(f"Unknown command: {args.command}")