# Running SCD2 Unit Tests

This sub-project contains the logic to repair the Hive Metastore. It works for both Hive Metastore 3.x and 4.x. 

In the 3.x version an `MSCK REPAIR` is called on the Hive Metastore services whereas in the 4.x version the Trino `sync_partition_metadata()` function is used.

## Run the comparision

Set environment variables for local environment

```bash
export HMS_MAJOR_VERSION=4
export HMS_HOST=localhost
export HMS_PORT=9083
export HMS_REST_PORT=9084

export TRINO_USER=trino
export TRINO_PASSWORD=
export TRINO_HOST=localhost
export TRINO_PORT=28082
export TRINO_CATALOG=iceberg_hive
export TRINO_SCHEMA=default
export TRINO_USE_SSL=false

export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=abc123abc123
export S3_ENDPOINT_URL=http://localhost:9000
export S3_PATH_STYLE_ACCESS=true
export S3_WAREHOUSE_BUCKET=warehouse-bucket
export S3_WAREHOUSE_PREFIX=warehouse
export S3_UPLOAD_BUCKET=admin-bucket
export S3_UPLOAD_PREFIX=warehouse
export UPLOAD_TO_S3=false
export DOWNLOAD_INITIAL_DATASET_FROM_S3=false
export DOWNLOAD_TEST_CASES_FROM_S3=false
```

Setup Python virual env

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r dev_requirements.txt
```

Specify the test engine to use, either `TRINO`, `SPARK` or `PYSPARK`

```bash
export TEST_ENGINE=TRINO
```