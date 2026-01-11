from datetime import datetime
import logging
import sys
import os

#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
from util import execute_with_metrics, get_table_data, render_table

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_prefix(values, prefix):
    """add a prefix to each value."""
    if prefix:
        return [f"{prefix}.{v}" for v in values]
    else:
        return values

def format_values(values):
    """Join column names with an optional prefix."""
    return ", ".join(f"{v}" for v in values)

def cast_to_varchar(values):
    """Cast column names to VARCHAR."""
    return [f"CAST({value} AS VARCHAR)" for value in values]

def format_create_dim_table(trino_catalog: str, trino_schema: str, table_name: str, s3_warehouse_bucket: str, s3_warehouse_prefix: str, pk_col_with_type:str, cols_with_type:list, partitioning_cols: list, sort_cols: list):

    cols_with_type_str = ", ".join([f"{col}" for col in cols_with_type]) if cols_with_type else ""
    partitioning_str = ", ".join([f"'{col}'" for col in partitioning_cols]) if partitioning_cols else ""
    sorted_by_str = ", ".join([f"'{col}'" for col in sort_cols]) if sort_cols else ""
    
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {trino_catalog}.{trino_schema}.{table_name} (
        {pk_col_with_type},
        {cols_with_type_str},

        -- SCD2 metadata columns
        dp_valid_from TIMESTAMP,
        dp_valid_to TIMESTAMP,
        dp_is_active BOOLEAN,
        dp_is_latest BOOLEAN,
        dp_load_timestamp TIMESTAMP,
        dp_created_at TIMESTAMP,
        dp_replaced_at TIMESTAMP,
        
        -- Additional metadata
        change_type VARCHAR,
        record_hash VARCHAR
    )
    WITH (
        partitioning = ARRAY[{partitioning_str}],
        sorted_by = ARRAY[{sorted_by_str}],
        location = 's3a://{s3_warehouse_bucket}/{s3_warehouse_prefix}/{trino_schema}/{table_name}'
    )
    """
    return ddl


def format_cte(trino_catalog: str, trino_schema: str, raw_table_name: str, dim_table_name: str, pk_col: str, val_columns: list, load_ts: datetime, load_ts_col: str):
    val_columns_str = format_values(val_columns)
    prefixed_val_columns_str = format_values(add_prefix(val_columns, "src"))
    cast_val_columns_str = format_values(cast_to_varchar(val_columns))
    load_ts_str = load_ts.strftime('%Y-%m-%d %H:%M:%S')

    stmt = f"""
    WITH changed_records AS (
        SELECT 
            CASE 
                WHEN src.{pk_col} IS NULL THEN tgt.{pk_col} 
                ELSE src.{pk_col}
            END AS {pk_col},
            {prefixed_val_columns_str},
            src.{load_ts_col} AS load_ts,
            src.status,
            CASE 
                WHEN tgt.{pk_col} IS NULL THEN 'NEW'
                WHEN src.{pk_col} IS NULL OR src.status = 'INACTIVE' THEN 'DELETED'
                WHEN src.row_hash != tgt.row_hash THEN 'CHANGED'
                ELSE 'UNCHANGED'
            END AS change_classification
        FROM (
            SELECT *,
                to_hex(
                    sha256(
                        CAST(
                            concat_ws('||', ARRAY[CAST({pk_col} AS VARCHAR), {cast_val_columns_str}, status])
                            AS VARBINARY
                        )
                    )
                ) AS row_hash
            FROM {trino_catalog}.{trino_schema}.{raw_table_name}
            WHERE {load_ts_col} = TIMESTAMP '{load_ts_str}'
        ) src
        FULL OUTER JOIN (
            SELECT 
                {pk_col},
                to_hex(
                    sha256(
                        CAST(
                            concat_ws('||', ARRAY[CAST({pk_col} AS VARCHAR), {cast_val_columns_str}, CASE WHEN dp_is_latest THEN 'ACTIVE' ELSE 'INACTIVE' END])
                            AS VARBINARY
                        )
                    )
                ) AS row_hash,
                dp_load_timestamp,
                dp_valid_from
            FROM {trino_catalog}.{trino_schema}.{dim_table_name}
            WHERE dp_is_active = TRUE
            AND dp_valid_to = TIMESTAMP '9999-12-31 23:59:59'
        ) tgt
        ON src.{pk_col} = tgt.{pk_col}
    ),
    records_to_process AS (
        SELECT *
        FROM changed_records
        WHERE change_classification IN ('NEW', 'CHANGED', 'DELETED')
    ),
    prepared_source AS (
        -- Original records for updates
        SELECT
            {pk_col} AS merge_key,
            {pk_col},
            {val_columns_str},
            load_ts,
            status,
            change_classification,
            'UPDATE_EXISTING' AS operation_type
        FROM records_to_process

        UNION ALL

        -- Duplicate records for inserts
        SELECT
            NULL AS merge_key,
            {pk_col},
            {val_columns_str},
            load_ts,
            status,
            change_classification,
            'INSERT_NEW_VERSION' AS operation_type
        FROM records_to_process
        WHERE change_classification = 'CHANGED'
    )
    """
    return stmt

def format_view(trino_catalog: str, trino_schema: str, raw_table_name: str, dim_table_name: str, scd2_view_name: str, pk_col: str, cols_with_type: list, load_ts: datetime, load_ts_col: str):

    val_columns = [col.split()[0] for col in cols_with_type]

    cte = format_cte(trino_catalog=trino_catalog, trino_schema=trino_schema, load_ts=load_ts, load_ts_col=load_ts_col, raw_table_name=raw_table_name, dim_table_name=dim_table_name, pk_col=pk_col, val_columns=val_columns)
    
    stmt = f"""
    CREATE OR REPLACE VIEW {trino_catalog}.{trino_schema}.{scd2_view_name} AS
        {cte}
    SELECT *
    FROM prepared_source
    """
    
    return stmt

def format_merge(load_ts: datetime, current_ts: datetime, trino_catalog: str, trino_schema: str, dim_table_name: str, scd2_view_name: str, pk_col: str, val_columns: list):
    prefixed_val_columns = add_prefix(val_columns, "source")

    val_columns_str = format_values(val_columns)
    source_val_columns_str = format_values(prefixed_val_columns)
    cast_source_val_columns_str = format_values(cast_to_varchar(prefixed_val_columns))    
    load_ts_str = load_ts.strftime('%Y-%m-%d %H:%M:%S')
    current_ts_str = current_ts.strftime('%Y-%m-%d %H:%M:%S')

    stmt = f"""

    MERGE INTO {trino_catalog}.{trino_schema}.{dim_table_name} AS target
    USING {trino_catalog}.{trino_schema}.{scd2_view_name} AS source
    ON target.{pk_col} = source.merge_key
    AND target.dp_is_active = TRUE
    AND target.dp_valid_to = TIMESTAMP '9999-12-31 23:59:59'

    WHEN MATCHED 
        AND source.operation_type = 'UPDATE_EXISTING'
        --AND source.load_ts > target.dp_load_timestamp
    THEN UPDATE SET
        dp_valid_to = CASE 
                            WHEN source.change_classification = 'DELETED'
                                THEN TIMESTAMP '{load_ts_str}' - INTERVAL '1' SECOND 
                            ELSE 
                                CAST(source.load_ts AS TIMESTAMP) - INTERVAL '1' SECOND 
                        END,
        dp_is_active = FALSE,
        dp_is_latest = CASE 
                            WHEN source.change_classification = 'DELETED'
                                THEN TRUE 
                            ELSE FALSE 
                        END,
        change_type = CASE WHEN source.change_classification = 'DELETED' 
                                THEN 'DELETED' 
                            ELSE 'SUPERSEDED' 
                        END,
        dp_replaced_at = TIMESTAMP '{current_ts_str}'

    WHEN NOT MATCHED
    THEN INSERT (
        {pk_col},
        {val_columns_str},
        dp_valid_from,
        dp_valid_to,
        dp_is_active,
        dp_is_latest,
        dp_load_timestamp,
        dp_created_at,
        dp_replaced_at,
        change_type,
        record_hash
    ) VALUES (
        source.{pk_col},
        {source_val_columns_str},
        source.load_ts,
        TIMESTAMP '9999-12-31 23:59:59',
        TRUE,
        TRUE,
        TIMESTAMP '{current_ts_str}',
        TIMESTAMP '{current_ts_str}',
        TIMESTAMP '9999-12-31 23:59:59',
        CASE 
            WHEN source.operation_type = 'UPDATE_EXISTING' THEN 'NEW'
            ELSE 'SUPERSEDED_BY'
        END,
        to_hex(
            sha256(
                CAST(
                    concat_ws('||', ARRAY[CAST(source.{pk_col} AS VARCHAR), {cast_source_val_columns_str}, source.status])
                    AS VARBINARY
                )
            )
        )
    )
    """

    return stmt

def create_dim_table(conn, trino_catalog: str, trino_schema: str, dim_table_name: str, s3_warehouse_bucket: str, s3_warehouse_prefix: str, pk_col_with_type: str, cols_with_type: list = None, partition_cols: list = None, sort_cols: list = None):

    drop_table_stmt = f"""DROP TABLE IF EXISTS {trino_catalog}.{trino_schema}.{dim_table_name}"""
    print(drop_table_stmt)
    execute_with_metrics(conn.cursor(), drop_table_stmt)

    create_table_stmt = format_create_dim_table(trino_catalog, trino_schema, dim_table_name, s3_warehouse_bucket, s3_warehouse_prefix, pk_col_with_type, cols_with_type, partition_cols, sort_cols)
    print(create_table_stmt)

    cursor = conn.cursor()
    cursor.execute(create_table_stmt)

    logger.info(f"Dimension table {dim_table_name} created successfully.")

def retrieve_iceberg_metadata(conn, trino_catalog: str, trino_schema: str, dim_table_name: str):
    
    query = f"""
            SELECT s.snapshot_id,
                    count(*) nof_files,
                    array_agg(case when e.status = 0 then 'existing' when e.status = 1 then 'added' when e.status = 2 then 'deleted' end) status_list,
                    array_agg(e.data_file.file_path) file_list
            FROM {trino_catalog}.{trino_schema}."{dim_table_name}$snapshots" s
            JOIN {trino_catalog}.{trino_schema}."{dim_table_name}$entries" e
            ON s.snapshot_id = e.snapshot_id
            WHERE s.snapshot_id in (select snapshot_id from {trino_catalog}.{trino_schema}."{dim_table_name}$snapshots" order by committed_at desc limit 1)
            AND e.status IN (0, 1, 2)
            GROUP BY s.snapshot_id
    """
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()

    return result

def merge_into_dim_table(conn, trino_catalog: str, trino_schema: str, raw_table_name: str, dim_table_name: str, scd2_view_name: str, pk_col: str, cols_with_type: list, load_ts: datetime, load_ts_col: str = "load_ts", current_ts: datetime = None, show_input_to_merge: bool = False, output_file_name: str = None):

    view_stmt = format_view(
        trino_catalog=trino_catalog,
        trino_schema=trino_schema,
        raw_table_name=raw_table_name,
        dim_table_name=dim_table_name,
        scd2_view_name=scd2_view_name,
        pk_col=pk_col,
        cols_with_type=cols_with_type,
        load_ts=load_ts,
        load_ts_col=load_ts_col
    )
    
    logger.debug (f"{view_stmt}")
    result = execute_with_metrics(conn.cursor(), view_stmt)
    logger.info("View creation result:", result)

    if show_input_to_merge:
        # For debugging: select from the view
        df = get_table_data(conn, f"{trino_catalog}.{trino_schema}.{scd2_view_name}", order_by_cols=["merge_key"])
        render_table(df, output_file_name=output_file_name, title="### Input to Merge")

    val_columns = [col.split()[0] for col in cols_with_type]
    merge_stmt = format_merge(
        load_ts=load_ts,
        current_ts=current_ts,
        trino_catalog=trino_catalog,
        trino_schema=trino_schema,
        dim_table_name=dim_table_name,
        scd2_view_name=scd2_view_name,
        pk_col=pk_col,
        val_columns=val_columns
    )

    logger.debug (f"{merge_stmt}")
    result = execute_with_metrics(conn.cursor(), merge_stmt)
    logger.info("Merge result:", result)
    # retrieve iceberg metadata after merge
    iceberg_metadata = retrieve_iceberg_metadata(conn, trino_catalog=trino_catalog, trino_schema=trino_schema, dim_table_name=dim_table_name)

    return result, iceberg_metadata