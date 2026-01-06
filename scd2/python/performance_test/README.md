# Hive Metastore Repair

This sub-project contains the logic to repair the Hive Metastore. It works for both Hive Metastore 3.x and 4.x. 

In the 3.x version an `MSCK REPAIR` is called on the Hive Metastore services whereas in the 4.x version the Trino `sync_partition_metadata()` function is used.

## Run the comparision

Set environment variables for local environment

```bash
export TSHIRT_SIZE="l"
export NOF_DAYS=30
export HMS_HOST=localhost
export HMS_PORT=9083

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
export S3_WAREHOUSE_PREFIX=warhouse
export S3_UPLOAD_BUCKET=admin-bucket
export S3_UPLOAD_PREFIX=warehouse
export UPLOAD_TO_S3=false
export DOWNLOAD_INITIAL_DATASET_FROM_S3=false
export DOWNLOAD_TEST_CASES_FROM_S3=false
```

Set environment variables for lightsail environment

```bash
export TSHIRT_SIZE="l"
export NOF_DAYS=30
export HMS_HOST=dataplatform
export HMS_PORT=9083

export TRINO_USER=trino
export TRINO_PASSWORD=
export TRINO_HOST=dataplatform
export TRINO_PORT=28082
export TRINO_CATALOG=iceberg_hive
export TRINO_SCHEMA=default
export TRINO_USE_SSL=false

export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=abc123abc123
export S3_ENDPOINT_URL=http://dataplatform:9000
export S3_PATH_STYLE_ACCESS=true
export S3_WAREHOUSE_BUCKET=warehouse-bucket
export S3_WAREHOUSE_PREFIX=iceberg_poc
export S3_UPLOAD_BUCKET=admin-bucket
export S3_UPLOAD_PREFIX=iceberg_poc
export UPLOAD_TO_S3=false
export DOWNLOAD_INITIAL_DATASET_FROM_S3=true
export DOWNLOAD_TEST_CASES_FROM_S3=true
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

```
nohup python3 run_iceberg_prepare_raw.py > output.log 2>&1 &
nohup python3 run_iceberg_scd2_merge.py > output.log 2>&1 &
```

The comparision is driven by the s3 locations in the file.


```sql
select person_id, count(*) from iceberg_hive."default".raw_person_m where operation in ('DELETE', 'UPDATE') group by person_id order by count(*) desc;

select * from iceberg_hive."default".raw_person_m where person_id = '418875'; 

select b.strategy,b.statement_name, b.case_id, b.tshirt_size, b.day_number
	, array_join(array_agg( format ('%.2f', round(cast (elapsed_ms as double) / 1000, 2))), ',') as elapsed_ms 
from iceberg_hive.default.benchmark b
where success = True
group by b.strategy,b.statement_name, b.case_id, b.tshirt_size, b.day_number
order by strategy,b.day_number asc;

select gender, count(*) from iceberg_hive."default".dim_person_5_m 
where is_current_version = true
group by gender;

select gender, count(*) from iceberg_hive."default".dim_person_5_m 
where is_current_version = true
group by gender;

select count(*) from iceberg_hive."default".dim_person_5_m 
where cast('2024-01-05' as date) between valid_from and valid_to
and is_current_version = true and is_active = true
and country = 'CH'

select array_join(array_agg(format('%.2f', round(cast (elapsed_ms as double) / 1000, 2))), ',')  from iceberg_hive."default".benchmark group by strategy

ALTER TABLE iceberg_hive."default".dim_person_1_m 
EXECUTE optimize (file_size_threshold => '128MB');
ALTER TABLE iceberg_hive."default".dim_person_1_m EXECUTE optimize_manifests;

ALTER TABLE iceberg_hive."default".dim_person_1_s EXECUTE expire_snapshots(retention_threshold => '7d');
```