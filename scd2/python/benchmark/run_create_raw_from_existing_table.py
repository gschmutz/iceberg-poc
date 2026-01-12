import sys
import os
import logging
import uuid
import boto3
import trino
import argparse
from trino.auth import BasicAuthentication
import random
from datetime import date, timedelta
from faker import Faker
import pandas as pd
import numpy as np
from pyiceberg.catalog import load_catalog
from datetime import date, timedelta, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
)
from pyiceberg.types import NestedField

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string, execute_with_metrics
from constants import DATE_FORMAT

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRINO_USER = get_credential('TRINO_USER', 'trino')
TRINO_PASSWORD = get_credential('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'iceberg_hive')
TRINO_SCHEMA = get_param('TRINO_SCHEMA', 'default')
TRINO_USE_SSL = get_param('TRINO_USE_SSL', 'true').lower() in ('true', '1', 't')

HMS_HOST = get_param('HMS_HOST', 'localhost')
HMS_PORT = get_param('HMS_PORT', '9083')

# Connect to MinIO or AWS S3
S3_ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')
S3_PATH_STYLE_ACCESS = get_param('S3_PATH_STYLE_ACCESS', 'true').lower() == 'true'

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
DOWNLOAD_INITIAL_DATASET_FROM_S3 = get_param('DOWNLOAD_INITIAL_DATASET_FROM_S3', 'true').lower() in ('true', '1', 't')

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

def format_create_raw_table(table_name: str) -> str:

    # setting timestamp to TIMSTAMP(6) as Iceberg always stores timestamps with microsecond precision

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{table_name} (
        clientdocumentid BIGINT,
        clientdocumentcreationdate TIMESTAMP(6),
        clientdocumentpriorityid BIGINT,
        clientdocumentpriorityenum VARCHAR,
        clientdocumentstatusid BIGINT,
        clientdocumentstatusenum VARCHAR,
        clientdocumentformid BIGINT,
        clientdocumentlabel VARCHAR,
        clientdocumentdescription VARCHAR,
        clientdocumentsignaturedate TIMESTAMP(6),
        clientdocumentobjectvers BIGINT, 
        clientdocumentdetails VARCHAR,
        clientdocumentnecessary INTEGER,
        clientdocumentmanualcreated INTEGER,
        clientdocumentmanualedited INTEGER,
        clientdocumentcreateduser VARCHAR,
        clientdocumentlifecyclestate BIGINT,
        clientdocumentfeeauthenticated INTEGER,
        clientdocumentdeviationtype BIGINT,
        clientdocumentwaiverlocation BIGINT,
        clientdocumentpledgeborrow INTEGER,
        clientdocumentvaliduntil TIMESTAMP(6),
        clientdocumentebaeruserid VARCHAR,
        clientdocumentownership BIGINT,
        clientdocumentsource BIGINT,
        clientdocumenthosttransmitid BIGINT,
        clientdocumentdispatchstate BIGINT,
        clientdocumenttaxstartdate TIMESTAMP(6),
        clientdocumentcollprovthird BIGINT,
        clientdocumentcrsescalation VARCHAR,
        clientdocumentfirstactivation TIMESTAMP(6),
        clientdocumentcrscarftype VARCHAR,
        dp_record_id VARCHAR,
        dp_load_timestamp TIMESTAMP(6),
        dp_valid_from DATE,
        dp_valid_to DATE,
        
        status VARCHAR,
        dp_exported_at TIMESTAMP(6)
    )
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['day(dp_exported_at)'],
        location = 's3a://{S3_WAREHOUSE_BUCKET}/{S3_WAREHOUSE_PREFIX}/{TRINO_SCHEMA}/{table_name}'   
    )
    """
    return ddl

def format_create_insert_table(source_table_name: str, target_table_name: str, dp_exported_at: str) -> str:
    
    ddl = f"""
    INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{target_table_name}
    SELECT
        clientdocumentid,
        CAST(clientdocumentcreationdate AS TIMESTAMP(6)) AS clientdocumentcreationdate,
        clientdocumentpriorityid,
        clientdocumentpriorityenum,
        clientdocumentstatusid,
        clientdocumentstatusenum,
        clientdocumentformid,
        clientdocumentlabel,
        clientdocumentdescription,
        CAST(clientdocumentsignaturedate AS TIMESTAMP(6)) AS clientdocumentsignaturedate,
        clientdocumentobjectvers, 
        clientdocumentdetails,
        CAST(clientdocumentnecessary AS INTEGER) AS clientdocumentnecessary,
        CAST(clientdocumentmanualcreated AS INTEGER) AS clientdocumentmanualcreated,
        CAST(clientdocumentmanualedited AS INTEGER) AS clientdocumentmanualedited,
        clientdocumentcreateduser,
        clientdocumentlifecyclestate,
        CAST(clientdocumentfeeauthenticated AS INTEGER) AS clientdocumentfeeauthenticated,
        clientdocumentdeviationtype,
        clientdocumentwaiverlocation,
        CAST(clientdocumentpledgeborrow AS INTEGER) AS clientdocumentpledgeborrow,
        CAST(clientdocumentvaliduntil AS TIMESTAMP(6)) AS clientdocumentvaliduntil,
        clientdocumentebaeruserid,
        clientdocumentownership,
        clientdocumentsource,
        clientdocumenthosttransmitid,
        clientdocumentdispatchstate,
        CAST(clientdocumenttaxstartdate AS TIMESTAMP(6)) AS clientdocumenttaxstartdate,
        clientdocumentcollprovthird,
        clientdocumentcrsescalation,
        CAST(clientdocumentfirstactivation AS TIMESTAMP(6)) AS clientdocumentfirstactivation,
        clientdocumentcrscarftype,
        dp_record_id,
        CAST(dp_load_timestamp AS TIMESTAMP(6)) AS dp_load_timestamp,
        dp_valid_from,
        dp_valid_to,
        'ACTIVE' AS status,
        CAST('{dp_exported_at}' AS TIMESTAMP) as dp_exported_at
    FROM hive.cur_zone.{source_table_name}
    WHERE CAST('{dp_exported_at}' AS TIMESTAMP) BETWEEN dp_valid_from AND dp_valid_to - INTERVAL '1' DAY
    """
    return ddl

def run_raw_create_table(table_name: str):

    #drop_table_stmt = f"""DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{table_name}"""
    #print(drop_table_stmt)
    #conn.cursor().execute(drop_table_stmt)
    #logger.info(f"Dropped raw table {table_name} if it existed.")

    create_table_stmt = format_create_raw_table(table_name)
    print(create_table_stmt)

    conn.cursor().execute(create_table_stmt)
    logger.info(f"Raw table {table_name} created successfully.")

def run_insert_from_existing_table(table_name: str, nof_days: int = 30):

    # create raw table
    target_table_name = f"raw_{table_name}"
    source_table_name = table_name

    # create raw table
    run_raw_create_table(target_table_name)

    # set the start date back to NOF_DAYS+1 ago
    start_date = date.today() - timedelta(days=nof_days+1)
    for d in range(nof_days):
        dp_exported_at = start_date + timedelta(days=d)
        print (dp_exported_at)

        stmt = format_create_insert_table(
            source_table_name=source_table_name,
            target_table_name=target_table_name,
            dp_exported_at=dp_exported_at.strftime(DATE_FORMAT)
        )
        print(stmt)
        conn.cursor().execute(stmt)
        logger.info(f"Inserted data from {source_table_name} to {target_table_name} successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", help="Command to run", default="run_insert_from_existing_table")
    parser.add_argument("--table_name", default="1", type=str)
    parser.add_argument("--nof_days", default=30, type=int)
    args = parser.parse_args()

    if args.command == "run_insert_from_existing_table":
        run_insert_from_existing_table(args.table_name)
    else:
        logger.error(f"Unknown command: {args.command}")