import sys
import os
import logging
import uuid
import boto3
import trino
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

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string, execute_with_metrics

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# size of benchmark
TSHIRT_SIZE = get_param('TSHIRT_SIZE', 'xl').lower()
NOF_DAYS = int(get_param('NOF_DAYS', '30'))

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

# 0.01 = 1% of data is xxxx
UPDATE_RATE = 0.005
INSERT_RATE = 0.05
DELETE_RATE = 0.001

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

arrow_schema = pa.schema([
    pa.field("surrogate_key", pa.string()),
    pa.field("person_id", pa.string()),

    pa.field("salutation", pa.string()),
    pa.field("title", pa.string()),
    pa.field("first_name", pa.string()),
    pa.field("middle_name", pa.string()),
    pa.field("last_name", pa.string()),
    pa.field("suffix", pa.string()),
    pa.field("gender", pa.string()),

    pa.field("email", pa.string()),
    pa.field("phone_mobile", pa.string()),
    pa.field("phone_home", pa.string()),

    pa.field("street", pa.string()),
    pa.field("house_number", pa.string()),
    pa.field("postal_code", pa.string()),
    pa.field("city", pa.string()),
    pa.field("state", pa.string()),
    pa.field("country", pa.string()),

    pa.field("birth_date", pa.date32()),
    pa.field("nationality", pa.string()),
    pa.field("marital_status", pa.string()),
    pa.field("number_of_children", pa.int32()),

    pa.field("employment_status", pa.string()),
    pa.field("job_title", pa.string()),
    pa.field("employer", pa.string()),
    pa.field("annual_income", pa.float64()),

    pa.field("national_id", pa.string()),
    pa.field("tax_id", pa.string()),

    pa.field("source_system", pa.string()),
    pa.field("status", pa.string()),
    pa.field("operation", pa.string()),

    pa.field("export_date", pa.date32(), nullable=True),
    pa.field("load_ts", pa.timestamp("us"), nullable=True),
])

iceberg_schema = Schema(
    NestedField(1, "surrogate_key", StringType(), required=False),
    NestedField(2, "person_id", StringType(), required=False),

    # identity
    NestedField(3, "salutation", StringType(), required=False),
    NestedField(4, "title", StringType(), required=False),
    NestedField(5, "first_name", StringType(), required=False),
    NestedField(6, "middle_name", StringType(), required=False),
    NestedField(7, "last_name", StringType(), required=False),
    NestedField(8, "suffix", StringType(), required=False),
    NestedField(9, "gender", StringType(), required=False),

    # contact
    NestedField(10, "email", StringType(), required=False),
    NestedField(11, "phone_mobile", StringType(), required=False),
    NestedField(12, "phone_home", StringType(), required=False),

    # address
    NestedField(13, "street", StringType(), required=False),
    NestedField(14, "house_number", StringType(), required=False),
    NestedField(15, "postal_code", StringType(), required=False),
    NestedField(16, "city", StringType(), required=False),
    NestedField(17, "state", StringType(), required=False),
    NestedField(18, "country", StringType(), required=False),

    # personal
    NestedField(19, "birth_date", DateType(), required=False),
    NestedField(20, "nationality", StringType(), required=False),
    NestedField(21, "marital_status", StringType(), required=False),
    NestedField(22, "number_of_children", IntegerType(), required=False),

    # employment
    NestedField(23, "employment_status", StringType(), required=False),
    NestedField(24, "job_title", StringType(), required=False),
    NestedField(25, "employer", StringType(), required=False),
    NestedField(26, "annual_income", DoubleType(), required=False),

    # identifiers
    NestedField(27, "national_id", StringType(), required=False),
    NestedField(28, "tax_id", StringType(), required=False),

    # metadata
    NestedField(29, "source_system", StringType(), required=False),
    NestedField(30, "status", StringType(), required=False),
    NestedField(31, "operation", StringType(), required=False),
    NestedField(32, "export_date", DateType(), required=False),
    NestedField(33, "load_ts", TimestampType(), required=False),
)

def format_create_raw_table(table_name: str) -> str:

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{table_name} (
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
        number_of_children INTEGER,

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
        status VARCHAR,
        operation VARCHAR,
        export_date DATE,
        load_ts TIMESTAMP
    )
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['day(export_date)'],
        location = 's3a://{S3_WAREHOUSE_BUCKET}/{S3_WAREHOUSE_PREFIX}/{TRINO_SCHEMA}/{table_name}'   
    )
    """
    return ddl

def generate_person_row(fake: Faker, person_id: int) -> dict:
    return {
        "surrogate_key": str(uuid.uuid4()),
        "person_id": str(person_id),

        "salutation": random.choice(["Mr", "Ms", "Mrs", "Dr"]),
        "title": random.choice(["", "Dr", "Prof"]),
        "first_name": fake.first_name(),
        "middle_name": fake.first_name() if random.random() < 0.3 else None,
        "last_name": fake.last_name(),
        "suffix": random.choice(["", "Jr", "Sr"]),
        "gender": random.choice(["M", "F", "X"]),

        "email": fake.email(),
        "phone_mobile": fake.phone_number(),
        "phone_home": fake.phone_number(),

        "street": fake.street_name(),
        "house_number": str(fake.building_number()),
        "postal_code": fake.postcode(),
        "city": fake.city(),
        "state": fake.state(),
        "country": fake.country_code(),

        "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=90),
        "nationality": fake.country_code(),
        "marital_status": random.choice(["single", "married", "divorced", "widowed"]),
        "number_of_children": random.randint(0, 4),

        "employment_status": random.choice(["employed", "self-employed", "unemployed", "retired"]),
        "job_title": fake.job(),
        "employer": fake.company(),
        "annual_income": round(random.uniform(30_000, 180_000), 2),

        "national_id": fake.ssn(),
        "tax_id": fake.bothify("??######"),

        "source_system": random.choice(["CRM", "ERP", "HR"]),
        "status": "ACTIVE",
        "operation": "INSERT"
    }

def apply_daily_changes(fake: Faker, df: pd.DataFrame, next_person_id: int):
    df['operation'] = 'INSERT'
    # Sample rows to update and delete
    updates = df.sample(frac=UPDATE_RATE, random_state=42)
    deletes = df.drop(updates.index).sample(frac=DELETE_RATE, random_state=24)

    # Apply updates
    updated = updates.copy()
    updated['operation'] = 'UPDATE'

    # Email update with 60% chance per row
    updated['email'] = updated['email'].where(
        np.random.rand(len(updated)) >= 0.6,
        [fake.email() for _ in range(len(updated))]
    )

    # Street update with 40% chance
    updated['street'] = updated['street'].where(
        np.random.rand(len(updated)) >= 0.4,
        [fake.street_name() for _ in range(len(updated))]
    )

    # Job title update with 30% chance
    updated['job_title'] = updated['job_title'].where(
        np.random.rand(len(updated)) >= 0.3,
        [fake.job() for _ in range(len(updated))]
    )

    # Annual income update with 30% chance
    mask = np.random.rand(len(updated)) < 0.3
    updated.loc[mask, 'annual_income'] = updated.loc[mask, 'annual_income'] * (
        1 + (np.random.rand(mask.sum()) - 0.5) / 5
    )

    logical_deleted = deletes.copy()
    logical_deleted['status'] = 'INACTIVE'
    logical_deleted['operation'] = 'DELETE'

    # Remove deleted and updated rows from the remaining dataset
    remaining = df[~df.index.isin(deletes.index) & ~df.index.isin(updates.index)]

    # Generate new inserts
    inserts_count = int(len(df) * INSERT_RATE)
    new_rows = [generate_person_row(fake, next_person_id + i) for i in range(inserts_count)]
    inserts = pd.DataFrame(new_rows)

    # Concatenate remaining, updated, logical_deleted, and new rows
    full_export = pd.concat([remaining, updated, logical_deleted, inserts], ignore_index=True)

    return full_export, next_person_id + inserts_count

def run_raw_create_table(table_name: str):

    drop_table_stmt = f"""DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{table_name}"""
    print(drop_table_stmt)
    conn.cursor().execute(drop_table_stmt)
    logger.info(f"Dropped raw table {table_name} if it existed.")

    create_table_stmt = format_create_raw_table(table_name)
    print(create_table_stmt)

    conn.cursor().execute(create_table_stmt)
    logger.info(f"Raw table {table_name} created successfully.")

def prepare_raw_data(use_hms: bool, generate_data: bool = True, initial_rows: int = 0):
    
    fake = Faker()
    Faker.seed(42)

    tshirt = TSHIRT_SIZE.lower()

    # create raw table
    table_name = f"raw_person_{tshirt}"

    if use_hms:
        # create raw table
        run_raw_create_table(table_name)

        # Prepare catalog properties with comprehensive S3 configuration
        catalog_props = {
            "name": "iceberg",
            "type": "hive",
            "uri": f"thrift://{HMS_HOST}:{HMS_PORT}",
            "warehouse": f"s3://{S3_WAREHOUSE_BUCKET}/{S3_WAREHOUSE_PREFIX}/",
            "s3.endpoint": S3_ENDPOINT_URL,
            "s3.path-style-access": S3_PATH_STYLE_ACCESS,  # Required for MinIO
        }
    else:
        catalog_props = {
            "name": "iceberg",
            "type": "in-memory",
            "warehouse": f"s3://{S3_WAREHOUSE_BUCKET}/warehouse",
            "s3.endpoint": S3_ENDPOINT_URL,
            "s3.path-style-access": S3_PATH_STYLE_ACCESS,
        }
    
    # Add AWS credentials if available
    if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
        catalog_props["s3.access-key-id"] = AWS_ACCESS_KEY
        catalog_props["s3.secret-access-key"] = AWS_SECRET_ACCESS_KEY
        
    catalog = load_catalog(**catalog_props)
    print(f"Catalog properties: {catalog.properties}")

    if (generate_data):
        # Generate initial data
        rows = [generate_person_row(fake, i) for i in range(initial_rows)]
        df = pd.DataFrame(rows)
        next_person_id = initial_rows
    else:
        local_file = f"data-{tshirt}.parquet"
    
        if DOWNLOAD_INITIAL_DATASET_FROM_S3:
            # Download the initial dataset from S3
            s3_key = f"{S3_UPLOAD_PREFIX}/initial-dataset/data-{tshirt}.parquet"
            logger.info(f"Downloading s3://{S3_UPLOAD_BUCKET}/{s3_key} to {local_file}")
            s3.download_file(S3_UPLOAD_BUCKET, s3_key, local_file)
            logger.info(f"Successfully downloaded {local_file} from S3")

        arrow_table = pq.read_table(local_file)
        df = arrow_table.to_pandas()
        next_person_id = df['person_id'].astype(int).max() + 1

    if use_hms:
        table = catalog.load_table(f"{TRINO_SCHEMA}.{table_name}")
    else:
        table_identifier = f"{table_name}"
        catalog.create_namespace(f"{TRINO_SCHEMA}")
        if not catalog.table_exists(table_identifier):
            table = catalog.create_table(
                identifier=table_identifier,
                schema=iceberg_schema,
                properties={
                    "format-version": "2",
                    "write.format.default": "parquet",
                }
            )
            logger.info(f"Created Iceberg table {table_identifier}")
        else:
            table = catalog.load_table(table_identifier)

    start_date = date(2024, 1, 1)

    for d in range(NOF_DAYS):
        export_date = start_date + timedelta(days=d)

        # 1️⃣ Apply daily updates / deletes / inserts
        df, next_person_id = apply_daily_changes(fake, df, next_person_id)

        # 2️⃣ Add export_date and load_ts columns
        df['export_date'] = export_date
        df['load_ts'] = datetime.now()

        # 3️⃣ Convert to Arrow Table
        arrow_table = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
        table.append(arrow_table)

        # 5️⃣ Print progress
        print(f"{export_date} | rows={len(df)} | next_person_id={next_person_id}")

prepare_raw_data(use_hms=True, generate_data=False, initial_rows=0)    