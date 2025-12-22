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
 

# Connect to MinIO or AWS S3
ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')

S3_ADMIN_BUCKET = get_param('S3_ADMIN_BUCKET', 'admin-bucket')
S3_ADMIN_BUCKET = replace_vars_in_string(S3_ADMIN_BUCKET, { "zone": "", "env": "" } )
S3_ADMIN_BUCKET_PREFIX = get_param('S3_ADMIN_BUCKET_PREFIX', '')

INITIAL_PERSONS = 1_000_000   # scale here
UPDATE_RATE = 0.10
INSERT_RATE = 0.05
DELETE_RATE = 0.005

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

    pa.field("export_date", pa.date32(), nullable=True),
    pa.field("load_ts", pa.timestamp("us"), nullable=True),
])

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
        "status": "ACTIVE"
    }

def apply_daily_changes(fake: Faker, df: pd.DataFrame, next_person_id: int):
    # Sample rows to update and delete
    updates = df.sample(frac=UPDATE_RATE, random_state=42)
    deletes = df.sample(frac=DELETE_RATE, random_state=24)

    # Apply updates
    updated = updates.copy()

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

    # Remove deleted and updated rows from the remaining dataset
    remaining = df[~df.index.isin(deletes.index) & ~df.index.isin(updates.index)]

    # Generate new inserts
    inserts_count = int(len(df) * INSERT_RATE)
    new_rows = [generate_person_row(fake, next_person_id + i) for i in range(inserts_count)]
    inserts = pd.DataFrame(new_rows)

    # Concatenate remaining, updated, and new rows
    full_export = pd.concat([remaining, updated, inserts], ignore_index=True)

    return full_export, next_person_id + inserts_count

def create_raw_data():
    fake = Faker()
    Faker.seed(42)

    # Prepare catalog properties with comprehensive S3 configuration
    catalog_props = {
        "name": "iceberg",
        "type": "hive",
        "uri": "thrift://localhost:9083",
        "warehouse": "s3://warehouse-bucket/",
        "s3.endpoint": ENDPOINT_URL,
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",  # Required for MinIO
    }
    
    # Add AWS credentials if available
    if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
        catalog_props["s3.access-key-id"] = AWS_ACCESS_KEY
        catalog_props["s3.secret-access-key"] = AWS_SECRET_ACCESS_KEY
        
    catalog = load_catalog(**catalog_props)
    print(f"Catalog properties: {catalog.properties}")
    print(f"Tables: {catalog.list_tables('default')}")


    rows = [generate_person_row(fake, i) for i in range(INITIAL_PERSONS)]
    df = pd.DataFrame(rows)

    next_person_id = INITIAL_PERSONS

    table = catalog.load_table("default.raw_person")

    start_date = date(2024, 1, 1)
    DAYS = 30

    for d in range(DAYS):
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




create_raw_data()    