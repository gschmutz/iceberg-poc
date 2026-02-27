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
        dp_key VARCHAR,
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


def format_cte(trino_catalog: str, trino_schema: str, raw_table_name: str, dim_table_name: str, pk_columns: list, val_columns: list, load_ts: datetime, load_ts_col: str, use_delta_mode_for_raw_table: bool = False):
    pk_columns_str = format_values(pk_columns)
    val_columns_str = format_values(val_columns)
    prefixed_val_columns_str = format_values(add_prefix(val_columns, "src"))
    cast_pk_columns_str = format_values(cast_to_varchar(pk_columns))
    cast_val_columns_str = format_values(cast_to_varchar(val_columns))
    load_ts_str = load_ts.strftime('%Y-%m-%d %H:%M:%S')

    stmt = f"""
    WITH changed_records AS (
        WITH src_records AS (
            SELECT *,
                to_hex(
                    sha256(
                        CAST(
                            concat_ws('||', ARRAY[{cast_pk_columns_str}, {cast_val_columns_str}])
                            AS VARBINARY
                        )
                    )
                ) AS row_hash
            FROM {trino_catalog}.{trino_schema}.{raw_table_name}
            WHERE {load_ts_col} = TIMESTAMP '{load_ts_str}'
        )
        SELECT
            CASE 
                WHEN src.{pk_columns_str} IS NULL THEN tgt.{pk_columns_str} 
                ELSE src.{pk_columns_str}
            END AS {pk_columns_str},
            {prefixed_val_columns_str},
            src.dp_valid_from   AS src_dp_valid_from,
            src.{load_ts_col}   AS load_ts,
            src.status,
            CASE 
                WHEN tgt.dp_key IS NULL AND prev.dp_key IS NULL AND active_succ.dp_key IS NULL THEN 'NEW'
                WHEN tgt.dp_key IS NULL AND prev.dp_valid_to = src.dp_valid_from - INTERVAL '1' second AND src.row_hash <> prev.row_hash THEN 'CHANGED_WITH_PREV_DIFF'
                WHEN tgt.dp_key IS NULL AND prev.dp_valid_to = src.dp_valid_from - INTERVAL '1' second AND src.row_hash = prev.row_hash THEN 'CHANGED_WITH_PREV_SAME'
                WHEN tgt.dp_key IS NULL AND prev.dp_valid_to < src.dp_valid_from AND src.row_hash <> prev.row_hash AND src.status = 'ACTIVE' THEN 'NEW_WITH_PREV_DIFF'
                WHEN tgt.dp_key IS NULL AND prev.dp_valid_to < src.dp_valid_from AND src.row_hash = prev.row_hash AND src.status = 'ACTIVE' THEN 'NEW_WITH_PREV_SAME'
                WHEN tgt.dp_key IS NULL AND src.row_hash <> active_succ.row_hash THEN 'NEW_WITH_SUCC_DIFF'
                WHEN tgt.dp_key IS NULL AND src.row_hash = active_succ.row_hash THEN 'NEW_WITH_SUCC_SAME'
                WHEN (( {use_delta_mode_for_raw_table}) OR src.status = 'INACTIVE') AND prev.dp_valid_to < src.dp_valid_from THEN 'DELETED_AGAIN_LATER_NOTHING_TO_DO'
                WHEN ( {use_delta_mode_for_raw_table}) OR src.status = 'INACTIVE' THEN 'DELETED'
                WHEN src.row_hash != tgt.row_hash THEN 'CHANGED'
                ELSE 'UNCHANGED'
            END AS change_classification,
            CASE
                WHEN tgt.dp_key IS NULL AND src.row_hash = prev.row_hash THEN prev.dp_key
                WHEN tgt.dp_key IS NULL AND src.row_hash <> prev.row_hash THEN prev.dp_key
                WHEN tgt.dp_key IS NULL AND src.row_hash = active_succ.row_hash THEN active_succ.dp_key
                WHEN tgt.dp_key IS NULL AND src.row_hash <> active_succ.row_hash THEN active_succ.dp_key
                ELSE tgt.dp_key
            END AS dp_key,
            COALESCE(tgt.dp_valid_from, TIMESTAMP '9999-12-31 23:59:59')     AS tgt_dp_valid_from,         -- not sure if we need valid_from downstream
            COALESCE(tgt.dp_valid_to, TIMESTAMP '9999-12-31 23:59:59')       AS tgt_dp_valid_to,           -- set the valid_to to max if null (not found in dim table)
            prev.dp_valid_from											     AS prev_dp_valid_from,
            prev.dp_valid_to											     AS prev_dp_valid_to,            
            active_succ.dp_valid_from									     AS succ_dp_valid_from,
            active_succ.dp_valid_to											 AS succ_dp_valid_to            
        FROM src_records AS src
        LEFT JOIN LATERAL (
            SELECT 
                {pk_columns_str},
                to_hex(
                    sha256(
                        CAST(
                            concat_ws('||', ARRAY[{cast_pk_columns_str}, {cast_val_columns_str}])
                            AS VARBINARY
                        )
                    )
                ) AS row_hash,
                dp_key,
                dp_load_timestamp,
                dp_valid_to,
                dp_valid_from         
            FROM {trino_catalog}.{trino_schema}.{dim_table_name}
            --WHERE (dp_is_active = TRUE AND dp_valid_to = TIMESTAMP '9999-12-31 23:59:59') OR dp_is_latest = true
            WHERE src.dp_valid_from BETWEEN dp_valid_from AND dp_valid_to
        ) tgt
        ON src.{pk_columns_str} = tgt.{pk_columns_str}
        LEFT JOIN LATERAL (
	        SELECT 
                dp_key,
                {pk_columns_str},
                to_hex(
                    sha256(
                        CAST(
                            concat_ws('||', ARRAY[{cast_pk_columns_str}, {cast_val_columns_str}])
                            AS VARBINARY
                        )
                    )
                ) AS row_hash,
                dp_valid_from,
	            dp_valid_to          
	        FROM {trino_catalog}.{trino_schema}.{dim_table_name}
	        WHERE dp_valid_to = src.dp_valid_from - INTERVAL '1' SECOND
            OR (dp_is_latest = TRUE AND dp_valid_to < src.dp_valid_from)
  	    ) prev 
  		ON (src.{pk_columns_str} = prev.{pk_columns_str})
        LEFT JOIN LATERAL (
	        SELECT 
                dp_key,
                {pk_columns_str},
                to_hex(
                    sha256(
                        CAST(
                            concat_ws('||', ARRAY[{cast_pk_columns_str}, {cast_val_columns_str}])
                            AS VARBINARY
                        )
                    )
                ) AS row_hash,
                dp_valid_from,
	            dp_valid_to          
	        FROM {trino_catalog}.{trino_schema}.{dim_table_name} AS succ
	        WHERE src.dp_valid_from < succ.dp_valid_from + INTERVAL '1' second
            AND succ.dp_is_active = TRUE
  	    ) active_succ 
  		ON (src.{pk_columns_str} = active_succ.{pk_columns_str})                     
    ),
    records_to_process AS (
        SELECT *
        FROM changed_records
        WHERE change_classification IN ('NEW', 'NEW_WITH_PREV_DIFF', 'NEW_WITH_PREV_SAME', 'NEW_WITH_SUCC_DIFF', 'NEW_WITH_SUCC_SAME', 'CHANGED_WITH_PREV_DIFF', 'CHANGED_WITH_PREV_SAME', 'CHANGED', 'DELETED')
    ),
    prepared_source AS (
        -- Original records for updates
        SELECT
            dp_key AS merge_key,
            dp_key,
            {pk_columns_str},
            {val_columns_str},
            src_dp_valid_from,
            load_ts,
            status,
            change_classification,
            'UPDATE_EXISTING' AS operation_type,
            tgt_dp_valid_from,
            tgt_dp_valid_to,
            prev_dp_valid_from,
            prev_dp_valid_to,
            succ_dp_valid_from,
            succ_dp_valid_to                  
        FROM records_to_process
        WHERE change_classification NOT IN ('NEW_WITH_SUCC_DIFF')       -- no update needed if there is an active successor with different value

        UNION ALL

        -- Duplicate records for inserts
        SELECT
            NULL AS merge_key,
            dp_key,
            {pk_columns_str},
            {val_columns_str},
            src_dp_valid_from,
            load_ts,
            status,
            change_classification,
            'INSERT_NEW_VERSION' AS operation_type,
            tgt_dp_valid_from,
            tgt_dp_valid_to,
            prev_dp_valid_from,
            prev_dp_valid_to,
            succ_dp_valid_from,
            succ_dp_valid_to                  
        FROM records_to_process
        WHERE change_classification IN ('CHANGED', 'NEW_WITH_PREV_DIFF', 'NEW_WITH_PREV_SAME', 'NEW_WITH_SUCC_DIFF', 'CHANGED_WITH_PREV_DIFF')
    )
    """
    return stmt

def format_view(trino_catalog: str, trino_schema: str, raw_table_name: str, dim_table_name: str, scd2_view_name: str, pk_col: str, cols_with_type: list, load_ts: datetime, load_ts_col: str, use_delta_mode_for_raw_table: bool = False):

    val_columns = [col.split()[0] for col in cols_with_type]

    cte = format_cte(trino_catalog=trino_catalog, trino_schema=trino_schema, load_ts=load_ts, load_ts_col=load_ts_col, raw_table_name=raw_table_name, dim_table_name=dim_table_name, pk_columns=[pk_col], val_columns=val_columns, use_delta_mode_for_raw_table=use_delta_mode_for_raw_table)
    
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
    ON target.dp_key = source.merge_key
    --AND ((target.dp_is_active = TRUE AND target.dp_valid_to = TIMESTAMP '9999-12-31 23:59:59') OR target.dp_is_latest = true)
    --AND  (src_dp_valid_from BETWEEN dp_valid_from AND dp_valid_to)
    WHEN MATCHED 
        AND source.operation_type = 'UPDATE_EXISTING'
        --AND source.load_ts > target.dp_load_timestamp
    THEN UPDATE SET
        dp_valid_from = CASE
                            WHEN source.change_classification = 'NEW_WITH_SUCC_SAME'
                                THEN source.src_dp_valid_from
                            ELSE
                                target.dp_valid_from
                        END,
        dp_valid_to = CASE 
                            WHEN source.change_classification = 'DELETED'
                                THEN src_dp_valid_from - INTERVAL '1' SECOND
                            WHEN source.change_classification = 'NEW_WITH_SUCC_SAME'
                                THEN source.tgt_dp_valid_to     
                            WHEN source.change_classification = 'CHANGED_WITH_PREV_SAME'
                                THEN source.tgt_dp_valid_to     
                            WHEN source.change_classification = 'CHANGED'
                                THEN CAST(source.load_ts AS TIMESTAMP) - INTERVAL '1' SECOND 
                            ELSE 
                                target.dp_valid_to  
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
        change_type = CASE WHEN source.change_classification = 'DELETED' 
                                THEN 'DELETED' 
                            ELSE 'SUPERSEDED' 
                        END,
        dp_replaced_at = TIMESTAMP '{current_ts_str}'    

    WHEN NOT MATCHED 
    THEN INSERT (
        dp_key,
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
        CAST( uuid() AS VARCHAR),
        source.{pk_col},
        {source_val_columns_str},
        source.src_dp_valid_from,
        CASE 
            WHEN source.change_classification = 'NEW_WITH_SUCC_DIFF' 
                THEN source.succ_dp_valid_from - INTERVAL '1' SECOND 
            ELSE source.tgt_dp_valid_to 
        END,
        CASE 
            WHEN change_classification = 'NEW_WITH_SUCC_DIFF' 
                THEN FALSE 
            WHEN source.tgt_dp_valid_to = TIMESTAMP '9999-12-31 23:59:59' 
                THEN TRUE 
            ELSE FALSE 
        END,    -- set the is_active to true, if valid_to is max timestamp
        CASE 
            WHEN change_classification = 'NEW_WITH_SUCC_DIFF' 
                THEN FALSE 
            WHEN source.tgt_dp_valid_to = TIMESTAMP '9999-12-31 23:59:59' 
                THEN TRUE 
            ELSE FALSE 
        END,    -- set the is_active to true, if valid_to is max timestamp
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
                    concat_ws('||', ARRAY[CAST(source.{pk_col} AS VARCHAR), {cast_source_val_columns_str}])
                    AS VARBINARY
                )
            )
        )
    ) 
    """
    print (stmt)
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

def optimize_table(conn, table_name: str):
    optimize_stmt = f"""
                    ALTER TABLE {table_name}
                    EXECUTE optimize (file_size_threshold => '256MB')
                    """
    print(optimize_stmt)
    result = execute_with_metrics(conn.cursor(), optimize_stmt)

    logger.info(f"Optimize table for {table_name} executed successfully.")


def run_analyze_table(conn, table_name: str):

    analyze_stmt = f"""
                    ANALYZE {table_name}
                    """
    print(analyze_stmt)
    result = execute_with_metrics(conn.cursor(), analyze_stmt)

    logger.info(f"Analyze table for {table_name} executed successfully.")

def merge_into_dim_table(conn, trino_catalog: str, trino_schema: str, raw_table_name: str, dim_table_name: str, scd2_view_name: str, pk_col: str, 
                         cols_with_type: list, load_ts: datetime, load_ts_col: str = "load_ts", current_ts: datetime = None, use_delta_mode_for_raw_table: bool = False, 
                         perform_merge_op: bool = True, show_input_to_merge: bool = False, output_file_name: str = None):

    view_stmt = format_view(
        trino_catalog=trino_catalog,
        trino_schema=trino_schema,
        raw_table_name=raw_table_name,
        dim_table_name=dim_table_name,
        scd2_view_name=scd2_view_name,
        pk_col=pk_col,
        cols_with_type=cols_with_type,
        load_ts=load_ts,
        load_ts_col=load_ts_col,
        use_delta_mode_for_raw_table=use_delta_mode_for_raw_table
    )
    
    logger.info (f"{view_stmt}")
    conn.cursor().execute(view_stmt)
    logger.info("View created successfully.")

    if show_input_to_merge:
        # For debugging: select from the view
        df = get_table_data(conn, f"{trino_catalog}.{trino_schema}.{scd2_view_name}", order_by_cols=["merge_key"])
        render_table(df, output_file_name=output_file_name, title="Input to Merge")

    if perform_merge_op:
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

        logger.info (f"{merge_stmt}")
        result = execute_with_metrics(conn.cursor(), merge_stmt)
        logger.info("Merge result:", result)
        # retrieve iceberg metadata after merge
        iceberg_metadata = retrieve_iceberg_metadata(conn, trino_catalog=trino_catalog, trino_schema=trino_schema, dim_table_name=dim_table_name)

        return result, iceberg_metadata
    else:
        return None, None