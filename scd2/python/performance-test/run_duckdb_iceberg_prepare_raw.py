import sys
import os
import logging
import random
import uuid
from datetime import date, timedelta

import trino
import duckdb
from duckdb.typing import VARCHAR, DATE
import pyarrow as pa
from faker import Faker
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string, execute_with_metrics

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

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

WAREHOUSE = "s3://warehouse-bucket/"

NAMESPACE = "default"

INITIAL_ROWS = 5_000_000
DAYS = 30

UPDATE_RATE = 0.10
INSERT_RATE = 0.05
DELETE_RATE = 0.005

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Trino setup
# ---------------------------------------------------------------------

# Construct connection URLs
conn = trino.dbapi.connect(
    host=f"{TRINO_HOST}",
    port=int(TRINO_PORT),
    user=f"{TRINO_USER}",
    catalog=f"{TRINO_CATALOG}",
    schema="default",
    http_scheme="http",
)

# ---------------------------------------------------------------------
# DuckDB setup
# ---------------------------------------------------------------------

fake = Faker()
Faker.seed(42)

con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=8")
con.execute("PRAGMA memory_limit='8GB'")

# ---------------------------------------------------------------------
# Register Faker UDFs (fast + vectorized)
# ---------------------------------------------------------------------

con.create_function("faker_first_name", lambda: fake.first_name(), return_type=VARCHAR)
con.create_function("faker_last_name", lambda: fake.last_name(), return_type=VARCHAR)
con.create_function("faker_email", lambda: fake.email(), return_type=VARCHAR)
con.create_function("faker_phone", lambda: fake.phone_number(), return_type=VARCHAR)
con.create_function("faker_street", lambda: fake.street_name(), return_type=VARCHAR)
con.create_function("faker_building", lambda: fake.building_number(), return_type=VARCHAR)
con.create_function("faker_postcode", lambda: fake.postcode(), return_type=VARCHAR)
con.create_function("faker_city", lambda: fake.city(), return_type=VARCHAR)
con.create_function("faker_state", lambda: fake.state(), return_type=VARCHAR)
con.create_function("faker_country_code", lambda: fake.country_code(), return_type=VARCHAR)
con.create_function("faker_birthdate", lambda: fake.date_of_birth(minimum_age=18, maximum_age=90), return_type=DATE)
con.create_function("faker_job", lambda: fake.job(), return_type=VARCHAR)
con.create_function("faker_company", lambda: fake.company(), return_type=VARCHAR)
con.create_function("faker_ssn", lambda: fake.ssn(), return_type=VARCHAR)
con.create_function("faker_taxid", lambda: fake.bothify("??######"), return_type=VARCHAR)

con.create_function(
    "random_choice",
    lambda x: random.choice(x),
    return_type="VARCHAR"
)
# ---------------------------------------------------------------------
# DuckDB table
# ---------------------------------------------------------------------

con.execute("""
CREATE TABLE persons (
    surrogate_key VARCHAR,
    person_id VARCHAR,

    salutation VARCHAR,
    title VARCHAR,
    first_name VARCHAR,
    middle_name VARCHAR,
    last_name VARCHAR,
    suffix VARCHAR,
    gender VARCHAR,

    email VARCHAR,
    phone_mobile VARCHAR,
    phone_home VARCHAR,

    street VARCHAR,
    house_number VARCHAR,
    postal_code VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,

    birth_date DATE,
    nationality VARCHAR,
    marital_status VARCHAR,
    number_of_children INTEGER,

    employment_status VARCHAR,
    job_title VARCHAR,
    employer VARCHAR,
    annual_income DOUBLE,

    national_id VARCHAR,
    tax_id VARCHAR,

    source_system VARCHAR,
    status VARCHAR
)
""")

def format_create_raw_table(table_name: str) -> str:

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
        export_date DATE,
        load_ts TIMESTAMP
    )
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['day(export_date)'],
        location = 's3a://warehouse-bucket/warehouse/default/{table_name}'   
    )
    """
    return ddl

def run_raw_create_table(table_name: str):

    drop_table_stmt = f"""DROP TABLE IF EXISTS iceberg_hive."default".{table_name}"""
    print(drop_table_stmt)
    execute_with_metrics(conn.cursor(), drop_table_stmt)

    create_table_stmt = format_create_raw_table(table_name)
    print(create_table_stmt)

    execute_with_metrics(conn.cursor(), create_table_stmt)
    logger.info(f"Raw table {table_name} created successfully.")

# ---------------------------------------------------------------------
# Initial load (set-based, fast)
# ---------------------------------------------------------------------

def initial_load(n):
    log.info("Initial load: %s rows", n)
    con.execute(f"""
    INSERT INTO persons
    SELECT
        uuid()::VARCHAR,
        row_number() OVER ()::VARCHAR,

        random_choice(['Mr','Ms','Mrs','Dr']),
        random_choice(['','Dr','Prof']),
        faker_first_name(),
        CASE WHEN random() < 0.3 THEN faker_first_name() ELSE NULL END,
        faker_last_name(),
        random_choice(['','Jr','Sr']),
        random_choice(['M','F','X']),

        faker_email(),
        faker_phone(),
        faker_phone(),

        faker_street(),
        faker_building(),
        faker_postcode(),
        faker_city(),
        faker_state(),
        faker_country_code(),

        faker_birthdate(),
        faker_country_code(),
        random_choice(['single','married','divorced','widowed']),
        floor(random() * 5),

        random_choice(['employed','self-employed','unemployed','retired']),
        faker_job(),
        faker_company(),
        round(random() * 150000 + 30000, 2),

        faker_ssn(),
        faker_taxid(),

        random_choice(['CRM','ERP','HR']),
        'ACTIVE'
    FROM range({n});
    """)

# ---------------------------------------------------------------------
# Daily CDC
# ---------------------------------------------------------------------

def apply_daily_changes(logical_del: bool = True):
    if logical_del:
        # Logical Deletes
        con.execute(f"""
        UPDATE persons
        SET status = 'INACTIVE'
        WHERE random() < {DELETE_RATE} and status = 'ACTIVE';
        """)
    else:
        # Deletes
        con.execute(f"""
        DELETE FROM persons
        WHERE random() < {DELETE_RATE};
        """)

    # Updates
    con.execute(f"""
    UPDATE persons
    SET
        email = CASE WHEN random() < 0.6 THEN faker_email() ELSE email END,
        street = CASE WHEN random() < 0.4 THEN faker_street() ELSE street END,
        job_title = CASE WHEN random() < 0.3 THEN faker_job() ELSE job_title END,
        annual_income = CASE
            WHEN random() < 0.3 THEN annual_income * (1 + (random() - 0.5) / 5)
            ELSE annual_income
        END
    WHERE random() < {UPDATE_RATE};
    """)

    # Inserts
    con.execute(f"""
    INSERT INTO persons
    SELECT
        uuid()::VARCHAR,
        (
            (SELECT max(person_id)::INTEGER FROM persons)
            + row_number() OVER ()
        )::VARCHAR,
        random_choice(['Mr','Ms','Mrs','Dr']),
        '',
        faker_first_name(),
        NULL,
        faker_last_name(),
        '',
        random_choice(['M','F','X']),

        faker_email(),
        faker_phone(),
        faker_phone(),

        faker_street(),
        faker_building(),
        faker_postcode(),
        faker_city(),
        faker_state(),
        faker_country_code(),

        faker_birthdate(),
        faker_country_code(),
        random_choice(['single','married','divorced','widowed']),
        floor(random() * 5),

        random_choice(['employed','self-employed','unemployed','retired']),
        faker_job(),
        faker_company(),
        round(random() * 150000 + 30000, 2),

        faker_ssn(),
        faker_taxid(),

        random_choice(['CRM','ERP','HR']),
        'ACTIVE'
    FROM range(
    CAST(
        (SELECT count(*) * {INSERT_RATE} FROM persons)
        AS INTEGER
        )
    );
    """)

# ---------------------------------------------------------------------
# Iceberg catalog
# ---------------------------------------------------------------------

def load_iceberg_table(table_name: str) -> Table:    
    # Prepare catalog properties with comprehensive S3 configuration
    catalog_props = {
        "name": "iceberg",
        "type": "hive",
        "uri": f"thrift://{HMS_HOST}:{HMS_PORT}",
        "warehouse": WAREHOUSE,
        "s3.endpoint": S3_ENDPOINT_URL,
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

    table = catalog.load_table(f"{NAMESPACE}.{table_name}")

    # Optimize file size
    with table.transaction() as tx:
        tx.set_properties({
            # File sizing
            "write.target-file-size-bytes": str(512 * 1024 * 1024),  # 512 MB
            "write.parquet.row-group-size-bytes": str(128 * 1024 * 1024),

            # Metadata
            "commit.retry.num-retries": "5",

            # Performance
            "write.distribution-mode": "hash",

            # Compression
            "write.parquet.compression-codec": "zstd"
        })
    return table

# ---------------------------------------------------------------------
# Iceberg append
# ---------------------------------------------------------------------

def append_to_iceberg(table: Table, export_date: date):
    arrow = con.execute("""
        SELECT *,
               CAST($export_date AS DATE) AS export_date,
               CAST(current_timestamp AS TIMESTAMP) AS load_ts
        FROM persons
    """, {"export_date": export_date}).fetch_arrow_table()

    table.append(arrow)

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

if __name__ == "__main__":
    tshirt = "xl"

    table_name = f"raw_person_{tshirt}"

    run_raw_create_table(table_name)

    table: Table = load_iceberg_table(table_name=table_name)

    initial_load(INITIAL_ROWS)

    start_date = date(2024, 1, 1)

    for d in range(DAYS):
        export_date = start_date + timedelta(days=d)

        apply_daily_changes(logical_del=True)
        append_to_iceberg(table, export_date)

        row_count = con.execute("SELECT count(*) FROM persons").fetchone()[0]
        log.info("Day %s | rows=%s", export_date, row_count)
