# Iceberg SCD2 PoC

Use the spark_sql catalog

```sql
USE spark_catalog;
```

To rollback to a previous snapshot

```sql
select * FROM spark_catalog.default.dim_person.snapshots order by committed_at desc;

CALL spark_catalog.system.rollback_to_snapshot(
  'spark_catalog.default.dim_person',
  1268188544570438362
);
```

Get the current snapshot id

```sql
SELECT snapshot_id
FROM spark_catalog.default.dim_person.snapshots
ORDER BY committed_at DESC
LIMIT 1;
```

## Create Raw and SCD2 tables

Drop and create the raw table

```sql
DROP TABLE raw_person;

CREATE TABLE raw_person (
    surrogate_key STRING,
    person_id    INT NOT NULL,
    first_name   STRING,
    last_name    STRING,
    city         STRING,
    email        STRING,
    load_date    DATE,
    status       STRING)
PARTITIONED BY (days(load_date))
LOCATION 's3a://admin-bucket/warehouse/raw_person';
```

Drop and create the dimensional table

```sql
DROP TABLE IF EXISTS dim_person;

-- Target SCD2 person table
CREATE TABLE IF NOT EXISTS dim_person (
    surrogate_key STRING,
    person_id    INT NOT NULL,
    first_name   STRING,
    last_name    STRING,
    city         STRING,
    email        STRING,
    
    -- SCD2 metadata columns
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current_version BOOLEAN,
    is_active BOOLEAN,
    source_loaded_at TIMESTAMP,
    created_at TIMESTAMP,
    replaced_at TIMESTAMP,
    
    -- Additional metadata
    change_type STRING,
    record_hash STRING
) USING ICEBERG
PARTITIONED BY (bucket(16, person_id), days(valid_to))
LOCATION 's3a://admin-bucket/warehouse/dim_person';
```

## Load the data into the `raw_person` table

Jupyter notebook: 


## Raw Table Setup




## Determine Changes from the full loads

The following statement can be used to select only the changes in `raw_person` from one load to another.

This is not feasible, as if we would load the same data twice without processing it, then we would not get any changes.

```sql
SET target_load_date = '2025-01-01';
SET target_load_date = '2025-01-05';
```

```sql
WITH prev_date AS (
  SELECT MAX(load_date) AS prev_load_date
  FROM raw_person
  WHERE load_date < CAST(${target_load_date} as date)
),
curr AS (
  SELECT
    *,
    sha2(concat_ws('||', person_id, first_name, last_name, city, email), 256) AS row_hash
  FROM raw_person
  WHERE load_date = CAST(${target_load_date} as date)
),
prev AS (
  SELECT
    *,
    sha2(concat_ws('||', person_id, first_name, last_name, city, email), 256) AS row_hash
  FROM raw_person
  WHERE load_date = (SELECT prev_load_date FROM prev_date)
)

SELECT
  c.*,
  COALESCE(c.person_id, p.person_id) AS person_id,
  CASE
    WHEN p.person_id IS NULL THEN 'NEW'
    WHEN c.person_id IS NULL THEN 'DELETED'
    WHEN c.row_hash <> p.row_hash THEN 'CHANGED'
    ELSE 'UNCHANGED'
  END AS diff_type
FROM curr c
FULL OUTER JOIN prev p
  ON c.person_id = p.person_id
WHERE
  p.person_id IS NULL
  OR c.person_id IS NULL
  OR c.row_hash <> p.row_hash;
```

## Create the SCD2 data




        SELECT 
            src.*,
            CASE 
                WHEN tgt.person_id IS NULL THEN 'NEW'
                WHEN src.val != tgt.val THEN 'CHANGED'
                ELSE 'UNCHANGED'
            END AS change_classification
        FROM (
            SELECT *,
            sha2(concat_ws('|', 
                    cast(person_id as string),
                    first_name,
                    last_name,
                    city,
                    email
                ), 256)    val             
            FROM raw_person src
            ) src
        LEFT JOIN (
            SELECT 
                person_id,
                source_loaded_at,
                effective_start_date,
                sha2(concat_ws('|', 
                  cast(person_id as string),
                     first_name,
                     last_name,
                     city,
                     email
                ), 256)    val                
            FROM dim_person 
            WHERE is_current_version = true
        ) tgt ON src.person_id = tgt.person_id
        WHERE src.load_date = CAST('2025-01-05' as date)


```sql
    WITH changed_records AS (
        SELECT 
            src.*,
            CASE 
                WHEN tgt.person_id IS NULL THEN 'NEW'
                WHEN src.load_date > tgt.source_loaded_at THEN 'CHANGED'
                ELSE 'UNCHANGED'
            END AS change_classification
        FROM raw_person src
        LEFT JOIN (
            SELECT 
                person_id,
                source_loaded_at,
                effective_start_date
            FROM dim_person 
            WHERE is_current_version = true
        ) tgt ON src.person_id = tgt.person_id
        WHERE src.load_date = CAST('2025-01-01' as date)
    ),
    records_to_process AS (
        SELECT *
        FROM changed_records
        WHERE change_classification IN ('NEW', 'CHANGED')
    ),
    prepared_source AS (
        -- Original records for matching existing rows (updates)
        SELECT 
            person_id AS merge_key,  -- Used for matching
            person_id,
            first_name,
            last_name,
            city,
            email,
            load_date,
            status,
            'UPDATE_EXISTING' AS operation_type
        FROM records_to_process

        UNION ALL

        -- Duplicate records with NULL key for insertions
        SELECT 
            NULL AS merge_key,     -- NULL prevents matching, forces insert
            person_id,
            first_name,
            last_name,
            city,
            email,
            load_date,
            status,
            'INSERT_NEW_VERSION' AS operation_type
        FROM records_to_process
        WHERE change_classification = 'CHANGED'  -- Only for updates, not new records
    )

    MERGE INTO dim_person target
    USING prepared_source source
    ON target.person_id = source.merge_key 
       AND target.is_current_version = true
    -- Close existing current records for updated entities
    WHEN MATCHED 
        AND source.operation_type = 'UPDATE_EXISTING'
        AND source.load_date > target.source_loaded_at
    THEN UPDATE SET
        effective_end_date = source.load_date,
        is_current_version = false,
        change_type = 'SUPERSEDED',
        pipeline_processed_at = current_timestamp()
    -- Insert new records (both new entities and new versions)
    WHEN NOT MATCHED 
    THEN INSERT (
        person_id,
        first_name,
        last_name,
        city,
        email,
        effective_start_date,
        effective_end_date,
        is_current_version,
        is_active,
        source_loaded_at,
        pipeline_processed_at,
        change_type,
        record_hash
    ) VALUES (
        source.person_id,
        source.first_name,
        source.last_name,
        source.city,
        source.email,
        source.load_date,
        CAST('9999-12-31 23:59:59' AS TIMESTAMP),  -- Far future date
        true,				-- is_current_version
        CASE WHEN source.status = 'ACTIVE' THEN true ELSE false END,
        source.load_date,
        current_timestamp(),
        CASE 
            WHEN source.operation_type = 'UPDATE_EXISTING' THEN 'NEW'
            ELSE 'NEW_VERSION'
        END,
        sha2(concat_ws('|', 
            cast(source.person_id as string),
            source.first_name,
            source.last_name,
            source.city,
            source.email
        ), 256)
    )
    -- Handle soft deletes for records no longer in source
    WHEN NOT MATCHED BY SOURCE 
        AND target.is_current_version = true 
        AND target.is_active = true
    THEN UPDATE SET
        is_active = false,
        change_type = 'DELETED',
        pipeline_processed_at = current_timestamp();
```        


```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Assume raw_person_df and dim_person_df are already loaded as DataFrames
# And dim_person is a Delta table

# Step 1: Identify changed records
dim_current_df = dim_person_df.filter(F.col("is_current_version") == True)\
    .select("person_id", "source_loaded_at", "effective_start_date")

changed_records_df = raw_person_df.filter(F.col("load_date") == F.lit("2025-01-01"))\
    .join(dim_current_df, on="person_id", how="left")\
    .withColumn(
        "change_classification",
        F.when(F.col("person_id").isNull(), "NEW")
         .when(F.col("load_date") > F.col("source_loaded_at"), "CHANGED")
         .otherwise("UNCHANGED")
    )

# Step 2: Filter only NEW or CHANGED records
records_to_process_df = changed_records_df.filter(
    F.col("change_classification").isin("NEW", "CHANGED")
)

# Step 3: Prepare source DataFrame for merge
update_existing_df = records_to_process_df.select(
    F.col("person_id").alias("merge_key"),  # used for matching
    "person_id",
    "first_name",
    "last_name",
    "city",
    "email",
    "load_date",
    "status"
).withColumn("operation_type", F.lit("UPDATE_EXISTING"))

insert_new_version_df = records_to_process_df.filter(F.col("change_classification") == "CHANGED")\
    .select(
        F.lit(None).cast("string").alias("merge_key"),  # prevents matching
        "person_id",
        "first_name",
        "last_name",
        "city",
        "email",
        "load_date",
        "status"
    ).withColumn("operation_type", F.lit("INSERT_NEW_VERSION"))

prepared_source_df = update_existing_df.unionByName(insert_new_version_df)

# Step 4: Merge into Delta table
delta_table = DeltaTable.forName(spark, "dim_person")

delta_table.alias("target").merge(
    prepared_source_df.alias("source"),
    "target.person_id = source.merge_key AND target.is_current_version = true"
).whenMatched(
    (F.col("source.operation_type") == "UPDATE_EXISTING") &
    (F.col("source.load_date") > F.col("target.source_loaded_at"))
).update(
    set={
        "effective_end_date": F.col("source.load_date"),
        "is_current_version": F.lit(False),
        "change_type": F.lit("SUPERSEDED"),
        "pipeline_processed_at": F.current_timestamp()
    }
).whenNotMatchedInsert(
    values={
        "person_id": F.col("source.person_id"),
        "first_name": F.col("source.first_name"),
        "last_name": F.col("source.last_name"),
        "city": F.col("source.city"),
        "email": F.col("source.email"),
        "effective_start_date": F.col("source.load_date"),
        "effective_end_date": F.lit("9999-12-31 23:59:59").cast("timestamp"),
        "is_current_version": F.lit(True),
        "is_active": F.when(F.col("source.status") == "ACTIVE", True).otherwise(False),
        "source_loaded_at": F.col("source.load_date"),
        "pipeline_processed_at": F.current_timestamp(),
        "change_type": F.when(F.col("source.operation_type") == "UPDATE_EXISTING", "NEW").otherwise("NEW_VERSION"),
        "record_hash": F.sha2(F.concat_ws("|",
            F.col("source.person_id").cast("string"),
            F.col("source.first_name"),
            F.col("source.last_name"),
            F.col("source.city"),
            F.col("source.email")
        ), 256)
    }
).execute()

# Step 5: Handle soft deletes for records no longer in source
# Get active current records not present in source
active_current_df = dim_person_df.filter((F.col("is_current_version") == True) & (F.col("is_active") == True))
soft_delete_df = active_current_df.join(prepared_source_df, active_current_df.person_id == prepared_source_df.person_id, "left_anti")

delta_table.alias("target").merge(
    soft_delete_df.alias("source"),
    "target.person_id = source.person_id AND target.is_current_version = true"
).whenMatchedUpdate(
    set={
        "is_active": F.lit(False),
        "change_type": F.lit("DELETED"),
        "pipeline_processed_at": F.current_timestamp()
    }
).execute()
```

## Create 

```
-- Source operational table
CREATE TABLE user_profiles (
    user_id BIGINT,
    email STRING,
    full_name STRING,
    subscription_tier STRING,
    registration_date TIMESTAMP,
    last_updated TIMESTAMP,
    status STRING
) USING ICEBERG
LOCATION 's3a://admin-bucket/warehouse/user_profiles';
```

INSERT INTO user_profiles VALUES (1, 'schmutz68@gmail.com', 'Guido Schmutz', 'GOLD', TIMESTAMP '2025-12-05 14:30:00',  current_timestamp(), 'ACTIVE');

INSERT INTO user_profiles VALUES (1, 'schmutz68@gmail.com', 'Guido Schmutz', 'SILVER', TIMESTAMP '2025-12-05 14:30:00',  current_timestamp(), 'ACTIVE');

DELETE FROM user_profiles WHERE id = 1;

INSERT INTO user_profiles VALUES (2, 'muster@gmail.com', 'Peter Muster', 'BRONZE', TIMESTAMP '2025-12-05 14:30:00',  current_timestamp(), 'ACTIVE');


```
-- Target SCD2 dimension table
CREATE TABLE dim_user_profiles (
    surrogate_key BIGINT,
    user_id BIGINT,
    email STRING,
    full_name STRING,
    subscription_tier STRING,
    registration_date TIMESTAMP,
    
    -- SCD2 metadata columns
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current_version BOOLEAN,
    is_active BOOLEAN,
    source_updated_at TIMESTAMP,
    pipeline_processed_at TIMESTAMP,
    
    -- Additional metadata
    change_type STRING,
    record_hash STRING
) USING ICEBERG
PARTITIONED BY (bucket(16, user_id), days(effective_start_date))
LOCATION 's3a://admin-bucket/warehouse/dim_user_profiles';
```

```
WITH changed_records AS (
    SELECT 
        src.*,
        CASE 
            WHEN tgt.user_id IS NULL THEN 'NEW'
            WHEN src.last_updated > tgt.source_updated_at THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END AS change_classification
    FROM user_profiles src
    LEFT JOIN (
        SELECT 
            user_id,
            source_updated_at,
            effective_start_date
        FROM dim_user_profiles 
        WHERE is_current_version = true
    ) tgt ON src.user_id = tgt.user_id
),
records_to_process AS (
    SELECT *
    FROM changed_records
    WHERE change_classification IN ('NEW', 'CHANGED')
),
prepared_source AS (
    -- Original records for matching existing rows (updates)
    SELECT 
        user_id AS merge_key,  -- Used for matching
        user_id,
        email,
        full_name,
        subscription_tier,
        registration_date,
        last_updated,
        status,
        'UPDATE_EXISTING' AS operation_type
    FROM records_to_process
    
    UNION ALL
    
    -- Duplicate records with NULL key for insertions
    SELECT 
        NULL AS merge_key,     -- NULL prevents matching, forces insert
        user_id,
        email,
        full_name,
        subscription_tier,
        registration_date,
        last_updated,
        status,
        'INSERT_NEW_VERSION' AS operation_type
    FROM records_to_process
    WHERE change_classification = 'CHANGED'  -- Only for updates, not new records
)

MERGE INTO dim_user_profiles target
USING prepared_source source
ON target.user_id = source.merge_key 
   AND target.is_current_version = true
-- Close existing current records for updated entities
WHEN MATCHED 
    AND source.operation_type = 'UPDATE_EXISTING'
    AND source.last_updated > target.source_updated_at
THEN UPDATE SET
    effective_end_date = source.last_updated,
    is_current_version = false,
    change_type = 'SUPERSEDED',
    pipeline_processed_at = current_timestamp()
-- Insert new records (both new entities and new versions)
WHEN NOT MATCHED 
THEN INSERT (
    user_id,
    email,
    full_name,
    subscription_tier,
    registration_date,
    effective_start_date,
    effective_end_date,
    is_current_version,
    is_active,
    source_updated_at,
    pipeline_processed_at,
    change_type,
    record_hash
) VALUES (
    source.user_id,
    source.email,
    source.full_name,
    source.subscription_tier,
    source.registration_date,
    source.last_updated,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP),  -- Far future date
    true,
    CASE WHEN source.status = 'ACTIVE' THEN true ELSE false END,
    source.last_updated,
    current_timestamp(),
    CASE 
        WHEN source.operation_type = 'UPDATE_EXISTING' THEN 'NEW'
        ELSE 'NEW_VERSION'
    END,
    sha2(concat_ws('|', 
        cast(source.user_id as string),
        source.email,
        source.full_name,
        source.subscription_tier
    ), 256)
)
-- Handle soft deletes for records no longer in source
WHEN NOT MATCHED BY SOURCE 
    AND target.is_current_version = true 
    AND target.is_active = true
THEN UPDATE SET
    is_active = false,
    change_type = 'DELETED',
    pipeline_processed_at = current_timestamp();
```

```
SELECT *
FROM user_profiles
WHERE last_updated > (
    SELECT COALESCE(MAX(source_updated_at), CAST('1900-01-01 00:00:00' AS TIMESTAMP))
    FROM dim_user_profiles
);
```

```
CREATE OR REPLACE TEMPORARY VIEW pipeline_metrics AS
WITH changed_records AS (
    SELECT 
        src.*,
        CASE 
            WHEN tgt.user_id IS NULL THEN 'NEW'
            WHEN src.last_updated > tgt.source_updated_at THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END AS change_classification
    FROM user_profiles src
    LEFT JOIN (
        SELECT 
            user_id,
            source_updated_at,
            effective_start_date
        FROM dim_user_profiles 
        WHERE is_current_version = true
    ) tgt ON src.user_id = tgt.user_id
),
records_to_process AS (
    SELECT *
    FROM changed_records
    WHERE change_classification IN ('NEW', 'CHANGED')
),
prepared_source AS (
    -- Original records for matching existing rows (updates)
    SELECT 
        user_id AS merge_key,  -- Used for matching
        user_id,
        email,
        full_name,
        subscription_tier,
        registration_date,
        last_updated,
        status,
        'UPDATE_EXISTING' AS operation_type
    FROM records_to_process
    
    UNION ALL
    
    -- Duplicate records with NULL key for insertions
    SELECT 
        NULL AS merge_key,     -- NULL prevents matching, forces insert
        user_id,
        email,
        full_name,
        subscription_tier,
        registration_date,
        last_updated,
        status,
        'INSERT_NEW_VERSION' AS operation_type
    FROM records_to_process
    WHERE change_classification = 'CHANGED'  -- Only for updates, not new records
), 
merge_stats AS (
    SELECT 
        current_timestamp() as execution_time,
        COUNT(*) as records_processed,
        COUNT(CASE WHEN operation_type = 'UPDATE_EXISTING' THEN 1 END) as records_updated,
        COUNT(CASE WHEN operation_type = 'INSERT_NEW_VERSION' THEN 1 END) as records_inserted,
        MAX(last_updated) as latest_source_timestamp
    FROM prepared_source
),
target_stats AS (
    SELECT 
        COUNT(*) as total_dimension_records,
        COUNT(CASE WHEN is_current_version = true THEN 1 END) as current_records,
        COUNT(CASE WHEN is_active = false THEN 1 END) as inactive_records
    FROM dim_user_profiles
)
SELECT 
    ms.*,
    ts.*,
    (CAST (ts.current_records AS float) / ts.total_dimension_records) * 100 as current_ratio_percent
FROM merge_stats ms
CROSS JOIN target_stats ts;
```




