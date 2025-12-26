# Hive Metastore Repair

This sub-project contains the logic to repair the Hive Metastore. It works for both Hive Metastore 3.x and 4.x. 

In the 3.x version an `MSCK REPAIR` is called on the Hive Metastore services whereas in the 4.x version the Trino `sync_partition_metadata()` function is used.

## Run the comparision

Set environment variables for local environment

```bash
export HMS_HOST=localhost
export HMS_PORT=9083

export TRINO_USER=trino
export TRINO_PASSWORD=
export TRINO_HOST=localhost
export TRINO_PORT=28082
export TRINO_CATALOG=iceberg_hive

export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=abc123abc123
export S3_ENDPOINT_URL=http://localhost:9000
export S3_ADMIN_BUCKET=admin-bucket
```

Set environment variables for lightsail environment

```bash
export HMS_HOST=dataplatform
export HMS_PORT=9083

export TRINO_USER=trino
export TRINO_PASSWORD=
export TRINO_HOST=dataplatform
export TRINO_PORT=28082
export TRINO_CATALOG=iceberg_hive

export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=abc123abc123
export S3_ENDPOINT_URL=http://dataplatform:9000
export S3_ADMIN_BUCKET=admin-bucket
```


```json
    "HMS_DB_HOST": "localhost",
    "HMS_DB_PORT": 5442,
    "HMS_DB_DBNAME": "metastore_db",

    "S3_ENDPOINT_URL": "http://minio-1:9000",
    "S3_BASELINE_BUCKET": "admin-bucket",
    "S3_BASELINE_OBJECT_NAME": "baseline_s3.csv"
```

```bash
    HMS_DB_USER=hive
    HMS_DB_PASSWORD=abc123!
```

Run `pytest`

```bash
pytest compare-partitions.py --verbose
```

The comparision is driven by the s3 locations in the file.