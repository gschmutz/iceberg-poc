from datetime import datetime
import logging
from typing import Optional

from scd2_strategy import SCD2Strategy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkSCD2Strategy(SCD2Strategy):
    """SCD2 strategy implementation for Apache Spark / Apache Iceberg.

    All SQL is generated in Spark SQL dialect (STRING casts, <=> null-safe
    equality, regular LEFT JOINs with conditions in the ON clause,
    upper(sha2(..., 256)) hashing, record_hash as surrogate key).

    The staging view is materialised as a Spark temp view via
    ``createOrReplaceTempView`` rather than as a catalog view.

    Usage::

        strategy = SparkSCD2Strategy(spark, database="default")
        strategy.create_dim_table(dim_table_name, ...)
        result, _ = strategy.merge_into_dim_table(raw_table_name, ...)
    """

    def __init__(self, spark, database: str):
        self.spark = spark
        self.database = database

    # ── Internal helpers ────────────────────────────────────────────────────

    def _fqn(self, table_name: str) -> str:
        """Return the fully-qualified Spark table name ``database.table``."""
        return f"{self.database}.{table_name}"

    @staticmethod
    def _cast_to_string(values: list) -> list:
        return [f"CAST({v} AS STRING)" for v in values]

    @staticmethod
    def _format_join_condition(pk_columns: list, prefix_left: str, prefix_right: str) -> str:
        return " AND ".join(
            f"{prefix_left}.{col} <=> {prefix_right}.{col}"
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
        location = 's3a://{s3_warehouse_bucket}/{s3_warehouse_prefix}/{self.database}/{table_name}'
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
        cs = self._cast_to_string

        prefixed_pk_columns_str = fv(ap(pk_columns, "src"))
        prefixed_val_columns_str = fv(ap(val_columns, "src"))
        load_ts_str = load_ts.strftime('%Y-%m-%d %H:%M:%S')

        join_src_tgt = self._format_join_condition(pk_columns, "src", "tgt")
        join_src_prev = self._format_join_condition(pk_columns, "src", "prev")
        join_src_succ = self._format_join_condition(pk_columns, "src", "active_succ")

        raw_fqn = self._fqn(raw_table_name)
        dim_fqn = self._fqn(dim_table_name)

        return f"""
    WITH changed_records AS (
        WITH src_records AS (
            SELECT {fv(ap(pk_columns, "t"))}, {fv(ap(val_columns, "t"))}, t.dp_ts_from, t.{load_ts_col}, t.status,
                upper(
                    sha2(
                        concat_ws('||', {fv(cs(ap(pk_columns, "t")))}, {fv(cs(ap(val_columns, "t")))}
                        ), 256
                    )
                ) AS record_hash
            FROM {raw_fqn} AS t
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
            COALESCE(tgt.dp_ts_from, TIMESTAMP '9999-12-31 23:59:59')     AS tgt_dp_ts_from,
            COALESCE(tgt.dp_ts_to, TIMESTAMP '9999-12-31 23:59:59')       AS tgt_dp_ts_to,
            prev.dp_ts_from                                               AS prev_dp_ts_from,
            prev.dp_ts_to                                                 AS prev_dp_ts_to,
            active_succ.dp_ts_from                                        AS succ_dp_ts_from,
            active_succ.dp_ts_to                                          AS succ_dp_ts_to
        FROM src_records AS src
        LEFT JOIN (
            SELECT
                {fv(ap(pk_columns, "tgt"))},
                tgt.record_hash,
                tgt.dp_key,
                tgt.dp_ts_to,
                tgt.dp_ts_from
            FROM {dim_fqn} AS tgt
        ) tgt
        ON {join_src_tgt}
        AND src.dp_ts_from BETWEEN tgt.dp_ts_from AND tgt.dp_ts_to
        LEFT JOIN (
            SELECT
                prev.dp_key,
                {fv(ap(pk_columns, "prev"))},
                prev.record_hash,
                prev.dp_ts_from,
                prev.dp_ts_to,
                prev.dp_is_latest
            FROM {dim_fqn} AS prev
        ) prev
        ON ({join_src_prev})
        AND (prev.dp_ts_to = src.dp_ts_from - INTERVAL '1' SECOND
            OR (prev.dp_is_latest = TRUE AND prev.dp_ts_to < src.dp_ts_from))
        LEFT JOIN (
            SELECT
                succ.dp_key,
                {fv(ap(pk_columns, "succ"))},
                succ.record_hash,
                succ.dp_ts_from,
                succ.dp_ts_to
            FROM {dim_fqn} AS succ
            WHERE succ.dp_is_active = TRUE
        ) active_succ
        ON ({join_src_succ})
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
            {fv(ap(pk_columns, "t"))},
            {fv(ap(val_columns, "t"))},
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
        FROM records_to_process t
        WHERE t.change_classification NOT IN ('NEW_WITH_SUCC_DIFF')
        UNION ALL
        -- Duplicate records for inserts
        SELECT
            NULL AS merge_key,
            t.dp_key,
            {fv(ap(pk_columns, "t"))},
            {fv(ap(val_columns, "t"))},
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
        FROM records_to_process t
        WHERE t.change_classification IN ('CHANGED', 'NEW_WITH_PREV_DIFF', 'NEW_WITH_PREV_SAME', 'NEW_WITH_SUCC_DIFF', 'CHANGED_WITH_PREV_DIFF')
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
        """Return the CTE + SELECT SQL to be run via spark.sql() and registered
        as a temp view with ``createOrReplaceTempView(scd2_view_name)``."""
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
        {fv(ap(pk_columns, "source"))},
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
        END,
        CASE
            WHEN change_classification = 'NEW_WITH_SUCC_DIFF'
                THEN FALSE
            WHEN source.tgt_dp_ts_to = TIMESTAMP '9999-12-31 23:59:59'
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
        self.spark.sql(drop_stmt)

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
        self.spark.sql(create_stmt)
        logger.info(f"Dimension table {dim_table_name} created successfully.")

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
        df = self.spark.sql(view_stmt)
        df.show(truncate=False)
        df.createOrReplaceTempView(scd2_view_name)
        logger.info(f"SCD2 view {scd2_view_name} created successfully.")

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
            try:
                result = self.spark.sql(merge_stmt)
                result.show(truncate=False)
            except Exception as e:
                logger.error(f"Error executing merge statement: {e}")
                result = None

            return result, None  # Spark has no Iceberg metadata query equivalent

        return None, None


# ── Backward-compatible module-level functions ──────────────────────────────
# New code should instantiate SparkSCD2Strategy directly.

def create_dim_table(
    spark,
    database: str,
    dim_table_name: str,
    s3_warehouse_bucket: str,
    s3_warehouse_prefix: str,
    pk_columns_with_type: list,
    cols_with_type: list = None,
    partition_cols: list = None,
    sort_cols: list = None,
) -> None:
    SparkSCD2Strategy(spark, database).create_dim_table(
        dim_table_name, s3_warehouse_bucket, s3_warehouse_prefix,
        pk_columns_with_type, cols_with_type, partition_cols, sort_cols,
    )


def merge_into_dim_table(
    spark,
    database: str,
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
    return SparkSCD2Strategy(spark, database).merge_into_dim_table(
        raw_table_name, dim_table_name, scd2_view_name,
        pk_columns, cols_with_type, load_ts, load_ts_col, current_ts,
        use_delta_mode_for_raw_table, perform_merge_op, show_input_to_merge, output_file_name,
    )
