import sys
import os
import logging
import uuid
import boto3
import trino
import random
from datetime import date, timedelta
from faker import Faker
import pandas as pd
import numpy as np
from pyiceberg.catalog import load_catalog
from datetime import date, timedelta, datetime

import pyarrow as pa
import pyarrow.parquet as pq

import s3fs
from sqlalchemy import create_engine, text
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string, execute_with_metrics

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
TRINO_USER = get_credential('TRINO_USER', 'trino')
TRINO_PASSWORD = get_credential('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'minio')
TRINO_USE_SSL = get_param('TRINO_USE_SSL', 'true').lower() in ('true', '1', 't')

HMS_HOST = get_param('HMS_HOST', 'localhost')
HMS_PORT = get_param('HMS_PORT', '9083')

# Connect to MinIO or AWS S3
S3_ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')

S3_ADMIN_BUCKET = get_param('S3_ADMIN_BUCKET', 'admin-bucket')
S3_ADMIN_BUCKET = replace_vars_in_string(S3_ADMIN_BUCKET, { "zone": "", "env": "" } )
AWS_ACCESS_KEY = get_credential('AWS_ACCESS_KEY', None)
AWS_SECRET_ACCESS_KEY = get_credential('AWS_SECRET_ACCESS_KEY', None)



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

if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
    s3_config["aws_access_key_id"] = AWS_ACCESS_KEY
    s3_config["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
if S3_ENDPOINT_URL:
    s3_config["endpoint_url"] = S3_ENDPOINT_URL
    s3_config["verify"] = False  # Disable SSL verification for self-signed certificates

s3 = boto3.client(**s3_config)

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

def create_initial_data(tshirt: str, initial_rows: int):
    fake = Faker()
    Faker.seed(42)

    rows = [generate_person_row(fake, i) for i in range(initial_rows)]
    df = pd.DataFrame(rows)

    table = pa.Table.from_pandas(
        df,
        preserve_index=False,   # 🔴 important
    )

    pq.write_table(
        table,
        f"data-{tshirt}.parquet",
        compression="zstd",     # 🔥 best compression
        compression_level=9,    # 🔥 compact but fast enough
        use_dictionary=True,    # 🔥 dictionary encoding
        data_page_size=1024 * 1024,  # 1 MB pages
    )

create_initial_data("xxl", 10_000_000)    