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

def cast_to_string(values):
    """Cast column names to STRING."""
    return [f"CAST({value} AS STRING)" for value in values]

def format_create_dim_table(database: str, table_name: str, s3_warehouse_bucket: str, s3_warehouse_prefix: str, pk_columns_with_type:list, cols_with_type:list, partitioning_cols: list, sort_cols: list):

    pk_columns_with_type_str = ", ".join([f"{col}" for col in pk_columns_with_type]) if pk_columns_with_type else ""
    cols_with_type_str = ", ".join([f"{col}" for col in cols_with_type]) if cols_with_type else ""
    partitioning_str = ", ".join([f"'{col}'" for col in partitioning_cols]) if partitioning_cols else ""
    sorted_by_str = ", ".join([f"'{col}'" for col in sort_cols]) if sort_cols else ""
    
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {database}.{table_name} (
        dp_key VARCHAR,
        {pk_columns_with_type_str},
        {cols_with_type_str},

        -- SCD2 metadata columns
        dp_ts_from TIMESTAMP,
        dp_ts_to TIMESTAMP,
        dp_is_active BOOLEAN,
        dp_is_latest BOOLEAN,
        dp_created_at TIMESTAMP,
        dp_replaced_at TIMESTAMP,
        
        -- Additional metadata
        record_hash VARCHAR
    )
    WITH (
        partitioning = ARRAY[{partitioning_str}],
        sorted_by = ARRAY[{sorted_by_str}],
        location = 's3a://{s3_warehouse_bucket}/{s3_warehouse_prefix}/{database}/{table_name}'
    )
    """
    return ddl

def format_join_condition(pk_columns: list, prefix_left: str, prefix_right: str):
    join_conditions = [f"{prefix_left}.{col} <=> {prefix_right}.{col}" for col in pk_columns]
    return " AND ".join(join_conditions)    

def format_cte(database: str, raw_table_name: str, dim_table_name: str, pk_columns: list, val_columns: list, load_ts: datetime, load_ts_col: str, use_delta_mode_for_raw_table: bool = False):
    pk_columns_str = format_values(pk_columns)
    prefixed_pk_columns_str = format_values(add_prefix(pk_columns, "src"))
    val_columns_str = format_values(val_columns)
    prefixed_val_columns_str = format_values(add_prefix(val_columns, "src"))
    cast_pk_columns_str = format_values(cast_to_string(pk_columns))
    cast_val_columns_str = format_values(cast_to_string(val_columns))
    load_ts_str = load_ts.strftime('%Y-%m-%d %H:%M:%S')

    stmt = f"""
    WITH changed_records AS (
        WITH src_records AS (
            SELECT {format_values(add_prefix(pk_columns, "t"))}, {format_values(add_prefix(val_columns, "t"))}, t.dp_ts_from, t.{load_ts_col}, t.status,
                upper(
                    sha2(
                        concat_ws('||', {format_values(cast_to_string(add_prefix(pk_columns, "t")))}, {format_values(cast_to_string(add_prefix(val_columns, "t")))}
                        ), 256
                    ) 
                ) AS record_hash
            FROM {database}.{raw_table_name} AS t
            WHERE {load_ts_col} = TIMESTAMP '{load_ts_str}'
        )
        SELECT
            {prefixed_pk_columns_str},
            {prefixed_val_columns_str},
            src.dp_ts_from      AS src_dp_ts_from,
            src.record_hash     AS src_record_hash,
            src.{load_ts_col}   AS load_ts,
            src.status,
            CASE 
                WHEN tgt.dp_key IS NULL AND prev.dp_key IS NULL AND active_succ.dp_key IS NULL THEN 'NEW'
                WHEN tgt.dp_key IS NULL AND prev.dp_ts_to = src.dp_ts_from - INTERVAL '1' second AND src.record_hash <> prev.record_hash THEN 'CHANGED_WITH_PREV_DIFF'
                WHEN tgt.dp_key IS NULL AND prev.dp_ts_to = src.dp_ts_from - INTERVAL '1' second AND src.record_hash = prev.record_hash THEN 'CHANGED_WITH_PREV_SAME'
                WHEN tgt.dp_key IS NULL AND prev.dp_ts_to < src.dp_ts_from AND src.record_hash <> prev.record_hash AND src.status = 'ACTIVE' THEN 'NEW_WITH_PREV_DIFF'
                WHEN tgt.dp_key IS NULL AND prev.dp_ts_to < src.dp_ts_from AND src.record_hash = prev.record_hash AND src.status = 'ACTIVE' THEN 'NEW_WITH_PREV_SAME'
                WHEN tgt.dp_key IS NULL AND src.record_hash <> active_succ.record_hash THEN 'NEW_WITH_SUCC_DIFF'
                WHEN tgt.dp_key IS NULL AND src.record_hash = active_succ.record_hash THEN 'NEW_WITH_SUCC_SAME'
                WHEN src.record_hash != tgt.record_hash and src.status != 'INACTIVE' THEN 'CHANGED'
                WHEN (( {use_delta_mode_for_raw_table}) OR src.status = 'INACTIVE') AND prev.dp_ts_to < src.dp_ts_from THEN 'DELETED_AGAIN_LATER_NOTHING_TO_DO'
                WHEN ( {use_delta_mode_for_raw_table}) OR src.status = 'INACTIVE' THEN 'DELETED'
                ELSE 'UNCHANGED'
            END AS change_classification,
            CASE
                WHEN tgt.dp_key IS NULL AND src.record_hash = prev.record_hash THEN prev.dp_key
                WHEN tgt.dp_key IS NULL AND src.record_hash <> prev.record_hash THEN prev.dp_key
                WHEN tgt.dp_key IS NULL AND src.record_hash = active_succ.record_hash THEN active_succ.dp_key
                WHEN tgt.dp_key IS NULL AND src.record_hash <> active_succ.record_hash THEN active_succ.dp_key
                ELSE tgt.dp_key
            END AS dp_key,
            COALESCE(tgt.dp_ts_from, TIMESTAMP '9999-12-31 23:59:59')     AS tgt_dp_ts_from,         -- not sure if we need valid_from downstream
            COALESCE(tgt.dp_ts_to, TIMESTAMP '9999-12-31 23:59:59')       AS tgt_dp_ts_to,           -- set the valid_to to max if null (not found in dim table)
            prev.dp_ts_from											      AS prev_dp_ts_from,
            prev.dp_ts_to											      AS prev_dp_ts_to,            
            active_succ.dp_ts_from									      AS succ_dp_ts_from,
            active_succ.dp_ts_to									      AS succ_dp_ts_to            
        FROM src_records AS src
        LEFT JOIN  (
            SELECT 
                {format_values(add_prefix(pk_columns, "tgt"))},
                tgt.record_hash,
                tgt.dp_key,
                tgt.dp_ts_to,
                tgt.dp_ts_from         
            FROM {database}.{dim_table_name} AS tgt
            --WHERE (dp_is_active = TRUE AND dp_ts_to = TIMESTAMP '9999-12-31 23:59:59') OR dp_is_latest = true
        ) tgt
        ON {format_join_condition(pk_columns, "src", "tgt")}
        AND src.dp_ts_from BETWEEN tgt.dp_ts_from AND tgt.dp_ts_to    
        LEFT JOIN  (
	        SELECT 
                prev.dp_key,
                {format_values(add_prefix(pk_columns, "prev"))},
                prev.record_hash,
                prev.dp_ts_from,
	            prev.dp_ts_to,
                prev.dp_is_latest       
	        FROM {database}.{dim_table_name} AS prev
  	    ) prev 
  		ON ({format_join_condition(pk_columns, "src", "prev")})
        AND (prev.dp_ts_to = src.dp_ts_from - INTERVAL '1' SECOND
            OR (prev.dp_is_latest = TRUE AND prev.dp_ts_to < src.dp_ts_from))
        LEFT JOIN (
	        SELECT 
                succ.dp_key,
                {format_values(add_prefix(pk_columns, "succ"))},
                succ.record_hash,
                succ.dp_ts_from,
	            succ.dp_ts_to          
	        FROM {database}.{dim_table_name} AS succ
            WHERE succ.dp_is_active = TRUE
  	    ) active_succ 
  		ON ({format_join_condition(pk_columns, "src", "active_succ")})                     
	    AND src.dp_ts_from < active_succ.dp_ts_from + INTERVAL '1' second
    ),
    records_to_process AS (
        SELECT *
        FROM changed_records AS t
        WHERE t.change_classification IN ('NEW', 'NEW_WITH_PREV_DIFF', 'NEW_WITH_PREV_SAME', 'NEW_WITH_SUCC_DIFF', 'NEW_WITH_SUCC_SAME', 'CHANGED_WITH_PREV_DIFF', 'CHANGED_WITH_PREV_SAME', 'CHANGED', 'DELETED')
    ),
    prepared_source AS (
        -- Original records for updates
        SELECT
            t.dp_key AS merge_key,
            t.dp_key,
            {format_values(add_prefix(pk_columns, "t"))},
            {format_values(add_prefix(val_columns, "t"))},
            t.src_dp_ts_from,
            t.src_record_hash as record_hash,
            t.load_ts,
            t.status,
            t.change_classification,
            'UPDATE_EXISTING' AS operation_type,
            t.tgt_dp_ts_from,
            t.tgt_dp_ts_to,
            t.prev_dp_ts_from,
            t.prev_dp_ts_to,
            t.succ_dp_ts_from,
            t.succ_dp_ts_to                  
        FROM records_to_process  t
        WHERE t.change_classification NOT IN ('NEW_WITH_SUCC_DIFF')       -- no update needed if there is an active successor with different value
        UNION ALL
        -- Duplicate records for inserts
        SELECT
            NULL AS merge_key,
            t.dp_key,
            {format_values(add_prefix(pk_columns, "t"))},
            {format_values(add_prefix(val_columns, "t"))},
            t.src_dp_ts_from,
            t.src_record_hash as record_hash,
            t.load_ts,
            t.status,
            t.change_classification,
            'INSERT_NEW_VERSION' AS operation_type,
            t.tgt_dp_ts_from,
            t.tgt_dp_ts_to,
            t.prev_dp_ts_from,
            t.prev_dp_ts_to,
            t.succ_dp_ts_from,
            t.succ_dp_ts_to                  
        FROM records_to_process  t
        WHERE t.change_classification IN ('CHANGED', 'NEW_WITH_PREV_DIFF', 'NEW_WITH_PREV_SAME', 'NEW_WITH_SUCC_DIFF', 'CHANGED_WITH_PREV_DIFF')
    )    
    """

    return stmt

def format_view(database: str, raw_table_name: str, dim_table_name: str, scd2_view_name: str, pk_columns: list, cols_with_type: list, load_ts: datetime, load_ts_col: str, use_delta_mode_for_raw_table: bool = False):

    val_columns = [col.split()[0] for col in cols_with_type]

    cte = format_cte(database=database, load_ts=load_ts, load_ts_col=load_ts_col, raw_table_name=raw_table_name, dim_table_name=dim_table_name, pk_columns=pk_columns, val_columns=val_columns, use_delta_mode_for_raw_table=use_delta_mode_for_raw_table)
    
    stmt = f"""
    CREATE OR REPLACE VIEW {database}.{scd2_view_name} AS
        {cte}
    SELECT *
    FROM prepared_source
    """
    
    stmt = f"""
    {cte}
    SELECT *
    FROM prepared_source
    """

    return stmt

def format_merge(load_ts: datetime, current_ts: datetime, database: str, dim_table_name: str, scd2_view_name: str, pk_columns: list, val_columns: list):
    prefixed_val_columns = add_prefix(val_columns, "source")

    pk_columns_str = format_values(pk_columns)
    val_columns_str = format_values(val_columns)
    source_val_columns_str = format_values(prefixed_val_columns)
    cast_pk_columns_str = format_values(cast_to_string(add_prefix(pk_columns, "source")))
    cast_source_val_columns_str = format_values(cast_to_string(prefixed_val_columns))    
    load_ts_str = load_ts.strftime('%Y-%m-%d %H:%M:%S')
    current_ts_str = current_ts.strftime('%Y-%m-%d %H:%M:%S')
    
    stmt = f"""

    MERGE INTO {database}.{dim_table_name} AS target
    USING {scd2_view_name} AS source
    ON target.dp_key = source.merge_key
    WHEN MATCHED 
        AND source.operation_type = 'UPDATE_EXISTING'
    THEN UPDATE SET
        dp_ts_from = CASE
                            WHEN source.change_classification = 'NEW_WITH_SUCC_SAME'
                                THEN source.src_dp_ts_from
                            ELSE
                                target.dp_ts_from
                        END,
        dp_ts_to = CASE 
                            WHEN source.change_classification = 'DELETED'
                                THEN src_dp_ts_from - INTERVAL '1' SECOND
                            WHEN source.change_classification = 'NEW_WITH_SUCC_SAME'
                                THEN source.tgt_dp_ts_to     
                            WHEN source.change_classification = 'CHANGED_WITH_PREV_SAME'
                                THEN source.tgt_dp_ts_to     
                            WHEN source.change_classification = 'CHANGED'
                                THEN CAST(source.src_dp_ts_from AS TIMESTAMP) - INTERVAL '1' SECOND 
                            ELSE 
                                target.dp_ts_to  
                        END,
        dp_is_active = CASE 
                            WHEN source.change_classification = 'NEW_WITH_SUCC_SAME'
                                THEN target.dp_is_active
                            WHEN source.change_classification = 'CHANGED_WITH_PREV_SAME'
                                THEN TRUE
                            ELSE
                                FALSE
                        END,
        dp_is_latest = CASE 
                            WHEN source.change_classification = 'DELETED'
                                THEN TRUE 
                            WHEN source.change_classification = 'NEW_WITH_SUCC_SAME'
                                THEN target.dp_is_latest
                            WHEN source.change_classification = 'CHANGED_WITH_PREV_SAME'
                                THEN TRUE
                            WHEN source.change_classification = 'CHANGED_WITH_PREV_DIFF' or source.change_classification = 'NEW_WITH_PREV_DIFF' or source.change_classification = 'NEW_WITH_PREV_SAME'
                                THEN FALSE
                            ELSE FALSE 
                        END,
        dp_replaced_at = TIMESTAMP '{current_ts_str}'    

    WHEN NOT MATCHED 
    THEN INSERT (
        dp_key,
        {pk_columns_str},
        {val_columns_str},
        dp_ts_from,
        dp_ts_to,
        dp_is_active,
        dp_is_latest,
        dp_created_at,
        dp_replaced_at,
        record_hash
    ) VALUES (
        source.record_hash,
        {format_values(add_prefix(pk_columns, "source"))},
        {source_val_columns_str},
        source.src_dp_ts_from,
        CASE 
            WHEN source.change_classification = 'NEW_WITH_SUCC_DIFF' 
                THEN source.succ_dp_ts_from - INTERVAL '1' SECOND 
            ELSE source.tgt_dp_ts_to 
        END,
        CASE 
            WHEN change_classification = 'NEW_WITH_SUCC_DIFF' 
                THEN FALSE 
            WHEN source.tgt_dp_ts_to = TIMESTAMP '9999-12-31 23:59:59' 
                THEN TRUE 
            ELSE FALSE 
        END,    -- set the is_active to true, if valid_to is max timestamp
        CASE 
            WHEN change_classification = 'NEW_WITH_SUCC_DIFF' 
                THEN FALSE 
            WHEN source.tgt_dp_ts_to = TIMESTAMP '9999-12-31 23:59:59' 
                THEN TRUE 
            ELSE FALSE 
        END,    -- set the is_active to true, if valid_to is max timestamp
        TIMESTAMP '{current_ts_str}',
        TIMESTAMP '9999-12-31 23:59:59',
        source.record_hash
    ) 
    """

    return stmt

def create_dim_table(spark, database: str, dim_table_name: str, s3_warehouse_bucket: str, s3_warehouse_prefix: str, pk_columns_with_type: list, cols_with_type: list = None, partition_cols: list = None, sort_cols: list = None):

    drop_table_stmt = f"""DROP TABLE IF EXISTS {database}.{dim_table_name}"""
    print(drop_table_stmt)
    spark.sql(drop_table_stmt)

    create_table_stmt = format_create_dim_table(database, dim_table_name, s3_warehouse_bucket, s3_warehouse_prefix, pk_columns_with_type, cols_with_type, partition_cols, sort_cols)
    print(create_table_stmt)

    spark.sql(create_table_stmt)
    logger.info(f"Dimension table {dim_table_name} created successfully.")

def merge_into_dim_table(spark, database:str, raw_table_name: str, dim_table_name: str, scd2_view_name: str, pk_columns: list, 
                         cols_with_type: list, load_ts: datetime, load_ts_col: str = "load_ts", current_ts: datetime = None, use_delta_mode_for_raw_table: bool = False, 
                         perform_merge_op: bool = True, show_input_to_merge: bool = False, output_file_name: str = None):

    view_stmt = format_view(
        database=database,
        raw_table_name=raw_table_name,
        dim_table_name=dim_table_name,
        scd2_view_name=scd2_view_name,
        pk_columns=pk_columns,
        cols_with_type=cols_with_type,
        load_ts=load_ts,
        load_ts_col=load_ts_col,
        use_delta_mode_for_raw_table=use_delta_mode_for_raw_table
    )

    print(view_stmt)
    df =spark.sql(view_stmt)
    df.show(truncate=False)
    df.createOrReplaceTempView(scd2_view_name)
    logger.info(f"SCD2 view {scd2_view_name} created successfully.")


    if perform_merge_op:
        val_columns = [col.split()[0] for col in cols_with_type]
        merge_stmt = format_merge(
            load_ts=load_ts,
            current_ts=current_ts,
            database=database,
            dim_table_name=dim_table_name,
            scd2_view_name=scd2_view_name,
            pk_columns=pk_columns,
            val_columns=val_columns
        )

        logger.info (f"{merge_stmt}")
        try:
            result = spark.sql(merge_stmt)
            result.show(truncate=False)
        except Exception as e:
            logger.error(f"Error executing merge statement: {e}")
            result = None
        #logger.info("Merge result:", result)
        # retrieve iceberg metadata after merge
        iceberg_metadata = None #retrieve_iceberg_metadata(conn, trino_catalog=trino_catalog, trino_schema=trino_schema, dim_table_name=dim_table_name)

        return result, iceberg_metadata
    else:
        return None, None