import sys
import os
import logging
import trino
import pytest
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential, replace_vars_in_string, render_table, render_data, get_table_data, diff_with_color

TRINO_USER = get_credential('TRINO_USER', 'trino')
TRINO_PASSWORD = get_credential('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'iceberg_hive')
TRINO_SCHEMA = get_param('TRINO_SCHEMA', 'default')
TRINO_USE_SSL = get_param('TRINO_USE_SSL', 'true').lower() in ('true', '1', 't')

@pytest.fixture(scope="function")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-pyspark")
        # Keep tests deterministic & reasonably fast
        .config("spark.ui.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.catalog.hiverest", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hiverest.type", "rest")
        .config("spark.sql.catalog.hiverest.uri", "http://localhost:9084/iceberg")
        .config("spark.sql.catalog.hiverest.token", "not_used")  # adjust as needed
        .config("spark.sql.catalog.hiverest.warehouse", "s3a://admin-bucket/iceberg/warehouse")  # adjust as needed
        .config("spark.sql.catalog.hiverest.s3.access-key-id", "admin")  # adjust as needed
        .config("spark.sql.catalog.hiverest.s3.secret-access-key", "abc123abc123")  # adjust as needed
        .config("spark.sql.catalog.hiverest.s3.endpoint", "http://localhost:9000")  # adjust as needed
        .config("spark.sql.catalog.hiverest.s3.path-style-access", "true")
        .config("spark.sql.catalog.hiverest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "hiverest")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()

@pytest.fixture(scope="function")
def trino_conn():
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
    )
    yield conn
    conn.close()