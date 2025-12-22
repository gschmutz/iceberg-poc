import sys
import os
import logging
import uuid
import boto3
import random
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
from sqlalchemy import create_engine, text
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string

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
trino_url = f'trino://{TRINO_USER}:{TRINO_PASSWORD}@{TRINO_HOST}:{TRINO_PORT}/{TRINO_CATALOG}'
if TRINO_USE_SSL:
    trino_url = f'{trino_url}?protocol=https&verify=false'

trino_engine = create_engine(trino_url)

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
    
def format_cte(load_date: str, table_name: str, pk_col: str, val_columns: list):
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
            FROM iceberg_hive.default.raw_person
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
            FROM iceberg_hive.default.dim_person
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

def format_view(load_date: str, table_name: str, pk_col: str, val_columns: list):
    cte = format_cte(load_date, table_name, pk_col, val_columns);
    stmt = f"""
    CREATE OR REPLACE VIEW minio.default.scd2_view AS
        {cte}
    SELECT *
    FROM prepared_source
    """
    return stmt

def format_merge(current_timestamp: str, table_name: str, pk_col: str, val_columns: list):
    prefixed_val_columns = add_prefix(val_columns, "source")

    val_columns_str = format_values(val_columns)
    source_val_columns_str = format_values(prefixed_val_columns)
    cast_source_val_columns_str = format_values(cast_to_varchar(prefixed_val_columns))    
    stmt = f"""


    MERGE INTO iceberg_hive.default.dim_person AS target
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


def run_dim_update(load_date: str):

    columns = [
        "salutation", "title", "first_name", "middle_name", "last_name", "suffix",
        "gender", "email", "phone_mobile", "phone_home", "street", "house_number",
        "postal_code", "city", "state", "country", "birth_date", "nationality",
        "marital_status", "number_of_children", "employment_status", "job_title",
        "employer", "annual_income", "national_id", "tax_id"
    ]

    view_stmt = format_view(
        load_date=load_date,
        table_name="dim_person",
        pk_col="person_id",
        val_columns=columns
    )

    merge_stmt = format_merge(
        current_timestamp="2025-12-18 12:00:00",
        table_name="dim_person",
        pk_col="person_id",
        val_columns=columns
    )


    with trino_engine.connect() as connection:

        connection.execute(text(view_stmt))

        connection.execute(text(merge_stmt))

run_dim_update(load_date="2024-01-12")