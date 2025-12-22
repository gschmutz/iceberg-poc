drop table if exists iceberg_hive."default".raw_person

CREATE TABLE IF NOT EXISTS iceberg_hive."default".raw_person (
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
    location = 's3a://warehouse-bucket/warehouse/default/raw_person'
    
);

drop table if exists iceberg_hive."default".raw_person;

CREATE TABLE IF NOT EXISTS iceberg_hive."default".raw_person (
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
    location = 's3a://warehouse-bucket/warehouse/default/raw_person'
    
);


drop table if exists iceberg_hive."default".dim_person;

CREATE TABLE IF NOT EXISTS iceberg_hive."default".dim_person (
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
    number_of_children INT,
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
    record_status VARCHAR,
    -- SCD2 metadata columns
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current_version BOOLEAN,
    is_active BOOLEAN,
    source_loaded_at TIMESTAMP,
    created_at TIMESTAMP,
    replaced_at TIMESTAMP,
    -- Additional metadata
    change_type VARCHAR,
    record_hash VARCHAR
)
WITH (
    partitioning = ARRAY['valid_from'],
    sorted_by = ARRAY['is_current_version'],
    location = 's3a://warehouse-bucket/warehouse/default/dim_person'
);
