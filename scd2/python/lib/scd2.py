from datetime import datetime
import logging
from typing import Optional

from scd2_strategy import SCD2Strategy
from util import execute_with_metrics, get_table_data, render_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TrinoSCD2Strategy(SCD2Strategy):
    """SCD2 strategy implementation for Trino / Apache Iceberg via Hive Metastore.

    All SQL is generated in Trino dialect (VARCHAR casts, LATERAL joins,
    IS NOT DISTINCT FROM null-safe equality, to_hex/sha256 hashing, uuid()
    surrogate key generation).

    Usage::

        strategy = TrinoSCD2Strategy(conn, catalog="iceberg_hive", schema="default")
        strategy.create_dim_table(dim_table_name, ...)
        result, metadata = strategy.merge_into_dim_table(raw_table_name, ...)
    """

    def __init__(self, conn, catalog: str, schema: str):
        self.conn = conn
        self.catalog = catalog
        self.schema = schema

    # ── Internal helpers ────────────────────────────────────────────────────

    def _fqn(self, table_name: str) -> str:
        """Return the fully-qualified Trino table name ``catalog.schema.table``."""
        return f"{self.catalog}.{self.schema}.{table_name}"

    @staticmethod
    def _cast_to_varchar(values: list) -> list:
        return [f"CAST({v} AS VARCHAR)" for v in values]

    @staticmethod
    def _format_join_condition(pk_columns: list, prefix_left: str, prefix_right: str) -> str:
        return " AND ".join(
            f"{prefix_left}.{col} IS NOT DISTINCT FROM {prefix_right}.{col}"
            for col in pk_columns
        )

    # ── Private SQL builders ────────────────────────────────────────────────

    def _format_create_dim_table(
        self,
        table_name: str,
        s3_warehouse_bucket: str,
        s3_warehouse_prefix: str,
        pk_columns_with_type: list,
        cols_with_type: Optional[list],
        partitioning_cols: Optional[list],
        sort_cols: Optional[list],
    ) -> str:
        pk_str = ", ".join(pk_columns_with_type) if pk_columns_with_type else ""
        cols_str = ", ".join(cols_with_type) if cols_with_type else ""
        partitioning_str = ", ".join(f"'{c}'" for c in partitioning_cols) if partitioning_cols else ""
        sorted_by_str = ", ".join(f"'{c}'" for c in sort_cols) if sort_cols else ""

        return f"""
    CREATE TABLE IF NOT EXISTS {self._fqn(table_name)} (
        dp_key VARCHAR,
        {pk_str},
        {cols_str},

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
        location = 's3a://{s3_warehouse_bucket}/{s3_warehouse_prefix}/{self.schema}/{table_name}'
    )
    """

    def _format_cte(
        self,
        raw_table_name: str,
        dim_table_name: str,
        pk_columns: list,
        val_columns: list,
        load_ts: datetime,
        load_ts_col: str,
        use_delta_mode_for_raw_table: bool = False,
    ) -> str:
        fv = self.format_values
        ap = self.add_prefix
        cv = self._cast_to_varchar

        pk_columns_str = fv(pk_columns)
        pk_prefixed_columns_str = fv(ap(pk_columns, "src"))
        val_columns_str = fv(val_columns)
        prefixed_val_columns_str = fv(ap(val_columns, "src"))
        cast_pk_columns_str = fv(cv(pk_columns))
        cast_val_columns_str = fv(cv(val_columns))
        load_ts_str = load_ts.strftime('%Y-%m-%d %H:%M:%S')

        join_src_overlap = self._format_join_condition(pk_columns, "src", "overlap")
        join_src_prev = self._format_join_condition(pk_columns, "src", "prev")
        join_src_next = self._format_join_condition(pk_columns, "src", "next")

        raw_fqn = self._fqn(raw_table_name)
        dim_fqn = self._fqn(dim_table_name)

        return f"""
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
                ) AS record_hash
            FROM {raw_fqn}
            WHERE {load_ts_col} = TIMESTAMP '{load_ts_str}'
        )
        SELECT
            {pk_prefixed_columns_str},
            {prefixed_val_columns_str},
            src.dp_ts_from      AS src_dp_ts_from,
            src.record_hash     AS src_record_hash,
            src.{load_ts_col}   AS load_ts,
            src.status,
            CASE
                WHEN overlap.dp_key IS NULL AND prev.dp_key IS NULL AND next.dp_key IS NULL THEN 'NEW'
                WHEN overlap.dp_key IS NULL AND prev.dp_ts_to = src.dp_ts_from - INTERVAL '1' second AND src.record_hash <> prev.record_hash THEN 'CHANGED_WITH_PREV_DIFF'
                WHEN overlap.dp_key IS NULL AND prev.dp_ts_to = src.dp_ts_from - INTERVAL '1' second AND src.record_hash = prev.record_hash THEN 'CHANGED_WITH_PREV_SAME'
                WHEN overlap.dp_key IS NULL AND prev.dp_ts_to < src.dp_ts_from AND src.record_hash <> prev.record_hash AND src.status = 'ACTIVE' THEN 'NEW_WITH_PREV_DIFF'
                WHEN overlap.dp_key IS NULL AND prev.dp_ts_to < src.dp_ts_from AND src.record_hash = prev.record_hash AND src.status = 'ACTIVE' THEN 'NEW_WITH_PREV_SAME'
                WHEN overlap.dp_key IS NULL AND src.record_hash <> next.record_hash THEN 'NEW_WITH_NEXT_DIFF'
                WHEN overlap.dp_key IS NULL AND src.record_hash = next.record_hash THEN 'NEW_WITH_NEXT_SAME'
                WHEN src.record_hash != overlap.record_hash and src.status != 'INACTIVE' THEN 'CHANGED'
                WHEN (( {use_delta_mode_for_raw_table}) OR src.status = 'INACTIVE') AND prev.dp_ts_to < src.dp_ts_from THEN 'DELETED_AGAIN_LATER_NOTHING_TO_DO'
                WHEN ( {use_delta_mode_for_raw_table}) OR src.status = 'INACTIVE' THEN 'DELETED'
                ELSE 'UNCHANGED'
            END AS change_classification,
            CASE
                WHEN overlap.dp_key IS NULL AND src.record_hash = prev.record_hash THEN prev.dp_key
                WHEN overlap.dp_key IS NULL AND src.record_hash <> prev.record_hash THEN prev.dp_key
                WHEN overlap.dp_key IS NULL AND src.record_hash = next.record_hash THEN next.dp_key
                WHEN overlap.dp_key IS NULL AND src.record_hash <> next.record_hash THEN next.dp_key
                ELSE overlap.dp_key
            END AS dp_key,
            COALESCE(overlap.dp_ts_from, TIMESTAMP '9999-12-31 23:59:59') AS overlap_dp_ts_from,
            COALESCE(overlap.dp_ts_to, TIMESTAMP '9999-12-31 23:59:59')   AS overlap_dp_ts_to,
            prev.dp_ts_from                                               AS prev_dp_ts_from,
            prev.dp_ts_to                                                 AS prev_dp_ts_to,
            next.dp_ts_from                                               AS next_dp_ts_from,
            next.dp_ts_to                                                 AS next_dp_ts_to
        FROM src_records AS src
        LEFT JOIN LATERAL (
            SELECT
                {pk_columns_str},
                record_hash,
                dp_key,
                dp_ts_to,
                dp_ts_from,
                dp_is_active,
                CASE WHEN src.record_hash = record_hash THEN TRUE ELSE FALSE END AS is_same_as_src,
                CASE WHEN record_hash IS NULL THEN 'NON_MATCHING_VERSION' ELSE 'MATCHING_VERSION' END AS match_classification
            FROM {dim_fqn}
            WHERE src.dp_ts_from BETWEEN dp_ts_from AND dp_ts_to
        ) overlap
        ON {join_src_overlap}
        LEFT JOIN LATERAL (
            SELECT
                dp_key,
                {pk_columns_str},
                record_hash,
                dp_ts_from,
                dp_ts_to,
                dp_is_active,
                CASE WHEN src.record_hash = record_hash THEN TRUE ELSE FALSE END AS is_same_as_src,
                CASE WHEN dp_ts_to = src.dp_ts_from - INTERVAL '1' SECOND THEN 'PREV_ENDS_WHEN_SRC_STARTS' ELSE 'PREV_HAS_GAP_WITH SRC_START' END AS prev_overlap_classification
            FROM {dim_fqn}
            WHERE dp_ts_to = src.dp_ts_from - INTERVAL '1' SECOND       -- previous version if it ends exactly when the new version starts
            OR (dp_ts_to < src.dp_ts_from AND dp_is_latest = TRUE)      -- we are interested in previous version even if it is not ending exactly at the new version, if it is the latest version
        ) prev
        ON ({join_src_prev})
        LEFT JOIN LATERAL (
            SELECT
                dp_key,
                {pk_columns_str},
                record_hash,
                dp_ts_from,
                dp_ts_to,
                dp_is_active,
                CASE WHEN src.record_hash = record_hash THEN TRUE ELSE FALSE END AS is_same_as_src
            FROM {dim_fqn} AS next
            WHERE src.dp_ts_from < next.dp_ts_from
            AND next.dp_is_active = TRUE
        ) next
        ON ({join_src_next})
    ),
    records_to_process AS (
        SELECT *
        FROM changed_records
        WHERE change_classification IN ('NEW', 'NEW_WITH_PREV_DIFF', 'NEW_WITH_PREV_SAME', 'NEW_WITH_NEXT_DIFF', 'NEW_WITH_NEXT_SAME', 'CHANGED_WITH_PREV_DIFF', 'CHANGED_WITH_PREV_SAME', 'CHANGED', 'DELETED')
    ),
    prepared_source AS (
        -- Original records for updates
        SELECT
            dp_key AS merge_key,
            dp_key,
            {pk_columns_str},
            {val_columns_str},
            src_dp_ts_from,
            src_record_hash as record_hash,
            load_ts,
            status,
            change_classification,
            'UPDATE_EXISTING' AS operation_type,
            overlap_dp_ts_from,
            overlap_dp_ts_to,
            prev_dp_ts_from,
            prev_dp_ts_to,
            next_dp_ts_from,
            next_dp_ts_to
        FROM records_to_process
        WHERE change_classification NOT IN ('NEW_WITH_NEXT_DIFF')

        UNION ALL

        -- Duplicate records for inserts
        SELECT
            NULL AS merge_key,
            dp_key,
            {pk_columns_str},
            {val_columns_str},
            src_dp_ts_from,
            src_record_hash as record_hash,
            load_ts,
            status,
            change_classification,
            'INSERT_NEW_VERSION' AS operation_type,
            overlap_dp_ts_from,
            overlap_dp_ts_to,
            prev_dp_ts_from,
            prev_dp_ts_to,
            next_dp_ts_from,
            next_dp_ts_to
        FROM records_to_process
        WHERE change_classification IN ('CHANGED', 'NEW_WITH_PREV_DIFF', 'NEW_WITH_PREV_SAME', 'NEW_WITH_NEXT_DIFF', 'CHANGED_WITH_PREV_DIFF')
    )
    """

    # ── Public SQL formatters (SCD2Strategy interface) ─────────────────────

    def format_view(
        self,
        raw_table_name: str,
        dim_table_name: str,
        scd2_view_name: str,
        pk_columns: list,
        cols_with_type: list,
        load_ts: datetime,
        load_ts_col: str,
        use_delta_mode_for_raw_table: bool = False,
    ) -> str:
        val_columns = [col.split()[0] for col in cols_with_type]
        cte = self._format_cte(
            raw_table_name=raw_table_name,
            dim_table_name=dim_table_name,
            pk_columns=pk_columns,
            val_columns=val_columns,
            load_ts=load_ts,
            load_ts_col=load_ts_col,
            use_delta_mode_for_raw_table=use_delta_mode_for_raw_table,
        )
        return f"""
    CREATE OR REPLACE VIEW {self._fqn(scd2_view_name)} AS
        {cte}
    SELECT *
    FROM prepared_source
    """

    def format_merge(
        self,
        current_ts: datetime,
        dim_table_name: str,
        scd2_view_name: str,
        pk_columns: list,
        val_columns: list,
    ) -> str:
        fv = self.format_values
        ap = self.add_prefix

        prefixed_val_columns = ap(val_columns, "source")
        pk_columns_str = fv(pk_columns)
        val_columns_str = fv(val_columns)
        source_val_columns_str = fv(prefixed_val_columns)
        current_ts_str = current_ts.strftime('%Y-%m-%d %H:%M:%S')

        return f"""
    MERGE INTO {self._fqn(dim_table_name)} AS target
    USING {self._fqn(scd2_view_name)} AS source
    ON target.dp_key = source.merge_key
    WHEN MATCHED
        AND source.operation_type = 'UPDATE_EXISTING'
    THEN UPDATE SET
        dp_ts_from = CASE
                            WHEN source.change_classification = 'NEW_WITH_NEXT_SAME'
                                THEN source.src_dp_ts_from
                            ELSE
                                target.dp_ts_from
                        END,
        dp_ts_to = CASE
                            WHEN source.change_classification = 'DELETED'
                                THEN src_dp_ts_from - INTERVAL '1' SECOND
                            WHEN source.change_classification = 'NEW_WITH_NEXT_SAME'
                                THEN source.overlap_dp_ts_to
                            WHEN source.change_classification = 'CHANGED_WITH_PREV_SAME'
                                THEN source.overlap_dp_ts_to
                            WHEN source.change_classification = 'CHANGED'
                                THEN CAST(source.src_dp_ts_from AS TIMESTAMP) - INTERVAL '1' SECOND
                            ELSE
                                target.dp_ts_to
                        END,
        dp_is_active = CASE
                            WHEN source.change_classification = 'NEW_WITH_NEXT_SAME'
                                THEN target.dp_is_active
                            WHEN source.change_classification = 'CHANGED_WITH_PREV_SAME'
                                THEN TRUE
                            ELSE
                                FALSE
                        END,
        dp_is_latest = CASE
                            WHEN source.change_classification = 'DELETED'
                                THEN TRUE
                            WHEN source.change_classification = 'NEW_WITH_NEXT_SAME'
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
        CAST( uuid() AS VARCHAR),
        {fv(ap(pk_columns, "source"))},
        {source_val_columns_str},
        source.src_dp_ts_from,
        CASE
            WHEN source.change_classification = 'NEW_WITH_NEXT_DIFF'
                THEN source.next_dp_ts_from - INTERVAL '1' SECOND
            ELSE source.overlap_dp_ts_to
        END,
        CASE
            WHEN change_classification = 'NEW_WITH_NEXT_DIFF'
                THEN FALSE
            WHEN source.overlap_dp_ts_to = TIMESTAMP '9999-12-31 23:59:59'
                THEN TRUE
            ELSE FALSE
        END,
        CASE
            WHEN change_classification = 'NEW_WITH_NEXT_DIFF'
                THEN FALSE
            WHEN source.overlap_dp_ts_to = TIMESTAMP '9999-12-31 23:59:59'
                THEN TRUE
            ELSE FALSE
        END,
        TIMESTAMP '{current_ts_str}',
        TIMESTAMP '9999-12-31 23:59:59',
        source.record_hash
    )
    """

    # ── Public operations (SCD2Strategy interface) ─────────────────────────

    def create_dim_table(
        self,
        dim_table_name: str,
        s3_warehouse_bucket: str,
        s3_warehouse_prefix: str,
        pk_columns_with_type: list,
        cols_with_type: Optional[list] = None,
        partition_cols: Optional[list] = None,
        sort_cols: Optional[list] = None,
    ) -> None:
        drop_stmt = f"DROP TABLE IF EXISTS {self._fqn(dim_table_name)}"
        print(drop_stmt)
        execute_with_metrics(self.conn.cursor(), drop_stmt)

        create_stmt = self._format_create_dim_table(
            table_name=dim_table_name,
            s3_warehouse_bucket=s3_warehouse_bucket,
            s3_warehouse_prefix=s3_warehouse_prefix,
            pk_columns_with_type=pk_columns_with_type,
            cols_with_type=cols_with_type,
            partitioning_cols=partition_cols,
            sort_cols=sort_cols,
        )
        print(create_stmt)
        self.conn.cursor().execute(create_stmt)
        logger.info(f"Dimension table {dim_table_name} created successfully.")

    def retrieve_iceberg_metadata(self, dim_table_name: str):
        query = f"""
            SELECT s.snapshot_id,
                    count(*) nof_files,
                    array_agg(case when e.status = 0 then 'existing' when e.status = 1 then 'added' when e.status = 2 then 'deleted' end) status_list,
                    array_agg(e.data_file.file_path) file_list
            FROM {self._fqn(f'"{dim_table_name}$snapshots"')} s
            JOIN {self._fqn(f'"{dim_table_name}$entries"')} e
            ON s.snapshot_id = e.snapshot_id
            WHERE s.snapshot_id in (select snapshot_id from {self._fqn(f'"{dim_table_name}$snapshots"')} order by committed_at desc limit 1)
            AND e.status IN (0, 1, 2)
            GROUP BY s.snapshot_id
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()

    def optimize_table(self, table_name: str) -> None:
        stmt = f"""
            ALTER TABLE {table_name}
            EXECUTE optimize (file_size_threshold => '256MB')
        """
        print(stmt)
        execute_with_metrics(self.conn.cursor(), stmt)
        logger.info(f"Optimize table for {table_name} executed successfully.")

    def run_analyze_table(self, table_name: str) -> None:
        stmt = f"ANALYZE {table_name}"
        print(stmt)
        execute_with_metrics(self.conn.cursor(), stmt)
        logger.info(f"Analyze table for {table_name} executed successfully.")

    def merge_into_dim_table(
        self,
        raw_table_name: str,
        dim_table_name: str,
        scd2_view_name: str,
        pk_columns: list,
        cols_with_type: list,
        load_ts: datetime,
        load_ts_col: str = "load_ts",
        current_ts: Optional[datetime] = None,
        use_delta_mode_for_raw_table: bool = False,
        perform_merge_op: bool = True,
        show_input_to_merge: bool = False,
        output_file_name: Optional[str] = None,
    ) -> tuple:
        view_stmt = self.format_view(
            raw_table_name=raw_table_name,
            dim_table_name=dim_table_name,
            scd2_view_name=scd2_view_name,
            pk_columns=pk_columns,
            cols_with_type=cols_with_type,
            load_ts=load_ts,
            load_ts_col=load_ts_col,
            use_delta_mode_for_raw_table=use_delta_mode_for_raw_table,
        )

        logger.info(view_stmt)
        self.conn.cursor().execute(view_stmt)
        logger.info("View created successfully.")

        if show_input_to_merge:
            df = get_table_data(
                self.conn,
                self._fqn(scd2_view_name),
                order_by_cols=["merge_key"],
            )
            render_table(df, output_file_name=output_file_name, title="Input to Merge")

        if perform_merge_op:
            val_columns = [col.split()[0] for col in cols_with_type]
            merge_stmt = self.format_merge(
                current_ts=current_ts,
                dim_table_name=dim_table_name,
                scd2_view_name=scd2_view_name,
                pk_columns=pk_columns,
                val_columns=val_columns,
            )

            logger.info(merge_stmt)
            result = execute_with_metrics(self.conn.cursor(), merge_stmt)
            logger.info(f"Merge result: {result}")
            iceberg_metadata = self.retrieve_iceberg_metadata(dim_table_name)
            return result, iceberg_metadata

        return None, None


# ── Backward-compatible module-level functions ──────────────────────────────
# These preserve the original call signatures so existing callers do not need
# to be updated.  New code should instantiate TrinoSCD2Strategy directly.

def add_prefix(values: list, prefix: str) -> list:
    return SCD2Strategy.add_prefix(values, prefix)


def format_values(values: list) -> str:
    return SCD2Strategy.format_values(values)


def cast_to_varchar(values: list) -> list:
    return TrinoSCD2Strategy._cast_to_varchar(values)


def format_join_condition(pk_columns: list, prefix_left: str, prefix_right: str) -> str:
    return TrinoSCD2Strategy._format_join_condition(pk_columns, prefix_left, prefix_right)


def format_create_dim_table(
    trino_catalog: str,
    trino_schema: str,
    table_name: str,
    s3_warehouse_bucket: str,
    s3_warehouse_prefix: str,
    pk_columns_with_type: list,
    cols_with_type: list,
    partitioning_cols: list,
    sort_cols: list,
) -> str:
    return TrinoSCD2Strategy(None, trino_catalog, trino_schema)._format_create_dim_table(
        table_name, s3_warehouse_bucket, s3_warehouse_prefix,
        pk_columns_with_type, cols_with_type, partitioning_cols, sort_cols,
    )


def format_cte(
    trino_catalog: str,
    trino_schema: str,
    raw_table_name: str,
    dim_table_name: str,
    pk_columns: list,
    val_columns: list,
    load_ts: datetime,
    load_ts_col: str,
    use_delta_mode_for_raw_table: bool = False,
) -> str:
    return TrinoSCD2Strategy(None, trino_catalog, trino_schema)._format_cte(
        raw_table_name, dim_table_name, pk_columns, val_columns,
        load_ts, load_ts_col, use_delta_mode_for_raw_table,
    )


def format_view(
    trino_catalog: str,
    trino_schema: str,
    raw_table_name: str,
    dim_table_name: str,
    scd2_view_name: str,
    pk_columns: list,
    cols_with_type: list,
    load_ts: datetime,
    load_ts_col: str,
    use_delta_mode_for_raw_table: bool = False,
) -> str:
    return TrinoSCD2Strategy(None, trino_catalog, trino_schema).format_view(
        raw_table_name, dim_table_name, scd2_view_name,
        pk_columns, cols_with_type, load_ts, load_ts_col, use_delta_mode_for_raw_table,
    )


def format_merge(
    current_ts: datetime,
    trino_catalog: str,
    trino_schema: str,
    dim_table_name: str,
    scd2_view_name: str,
    pk_columns: list,
    val_columns: list,
) -> str:
    return TrinoSCD2Strategy(None, trino_catalog, trino_schema).format_merge(
        current_ts, dim_table_name, scd2_view_name, pk_columns, val_columns,
    )


def create_dim_table(
    conn,
    trino_catalog: str,
    trino_schema: str,
    dim_table_name: str,
    s3_warehouse_bucket: str,
    s3_warehouse_prefix: str,
    pk_columns_with_type: list,
    cols_with_type: list = None,
    partition_cols: list = None,
    sort_cols: list = None,
) -> None:
    TrinoSCD2Strategy(conn, trino_catalog, trino_schema).create_dim_table(
        dim_table_name, s3_warehouse_bucket, s3_warehouse_prefix,
        pk_columns_with_type, cols_with_type, partition_cols, sort_cols,
    )


def retrieve_iceberg_metadata(
    conn,
    trino_catalog: str,
    trino_schema: str,
    dim_table_name: str,
):
    return TrinoSCD2Strategy(conn, trino_catalog, trino_schema).retrieve_iceberg_metadata(
        dim_table_name
    )


def optimize_table(conn, table_name: str) -> None:
    stmt = f"""
        ALTER TABLE {table_name}
        EXECUTE optimize (file_size_threshold => '256MB')
    """
    print(stmt)
    execute_with_metrics(conn.cursor(), stmt)
    logger.info(f"Optimize table for {table_name} executed successfully.")


def run_analyze_table(conn, table_name: str) -> None:
    stmt = f"ANALYZE {table_name}"
    print(stmt)
    execute_with_metrics(conn.cursor(), stmt)
    logger.info(f"Analyze table for {table_name} executed successfully.")


def merge_into_dim_table(
    conn,
    trino_catalog: str,
    trino_schema: str,
    raw_table_name: str,
    dim_table_name: str,
    scd2_view_name: str,
    pk_columns: list,
    cols_with_type: list,
    load_ts: datetime,
    load_ts_col: str = "load_ts",
    current_ts: datetime = None,
    use_delta_mode_for_raw_table: bool = False,
    perform_merge_op: bool = True,
    show_input_to_merge: bool = False,
    output_file_name: str = None,
) -> tuple:
    return TrinoSCD2Strategy(conn, trino_catalog, trino_schema).merge_into_dim_table(
        raw_table_name, dim_table_name, scd2_view_name,
        pk_columns, cols_with_type, load_ts, load_ts_col, current_ts,
        use_delta_mode_for_raw_table, perform_merge_op, show_input_to_merge, output_file_name,
    )
