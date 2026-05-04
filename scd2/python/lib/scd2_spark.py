import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from pyspark.sql.types import TimestampNTZType, TimestampType
from pyspark.sql import DataFrame
from scd2_strategy import SCD2Strategy, SCD2Table
from util import render_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkSCD2Strategy(SCD2Strategy):
    """SCD2 strategy implementation for Apache Spark / Apache Iceberg.

    All SQL is generated in Spark SQL dialect (STRING casts, <=> null-safe  
    equality, regular LEFT JOINs with conditions in the ON clause,
    upper(sha2(..., 256)) hashing, dp_record_hash as surrogate key).

    The staging view is materialised as a Spark temp view via
    ``createOrReplaceTempView`` rather than as a catalog view.

    Usage::

        strategy = SparkSCD2Strategy(spark, database="default")
        strategy.create_scd2_table(dim_table_name, ...)
        result, _ = strategy.merge_into_scd2_table(raw_table_name, ...)
    """

    def __init__(
        self,
        spark,
        database: str,
        raw_table_name: str,
        scd2_table_name: str,
        scd2_intermediary_table_name: str = None,
        cols_bks: Optional[list] = None,
        cols_val: Optional[list] = None,
        use_delta_mode_for_raw_table: bool = False,
        materialize_data_before_merge: bool = True,
        perform_merge_op: bool = True,
        col_dp_valid_from: str = "dp_ts_from",
        col_dp_valid_to: str = "dp_ts_to",
        col_dp_created_at: str = "dp_created_at",
        col_dp_replaced_at: str = "dp_replaced_at",        
        col_dp_ts: str = "dp_ts_version",
        col_dp_ts_filter: str = "dp_ts",
    ):
        super().__init__(
            scd2_intermediary_table_name=(
                    scd2_intermediary_table_name or f"{scd2_table_name}_temp"
                    ),
            cols_bks=cols_bks,
            cols_val=cols_val,
            use_delta_mode_for_raw_table=use_delta_mode_for_raw_table,
            materialize_data_before_merge=materialize_data_before_merge,
            perform_merge_op=perform_merge_op,
            col_dp_valid_from=col_dp_valid_from,
            col_dp_valid_to=col_dp_valid_to,
            col_dp_created_at=col_dp_created_at,
            col_dp_replaced_at=col_dp_replaced_at,
            col_dp_ts=col_dp_ts,
            col_dp_ts_filter=col_dp_ts_filter,
        )
        self.spark = spark
        self.database = database
        self.raw_table_name = raw_table_name
        self.scd2_table_name = scd2_table_name

    # ── Internal helpers ────────────────────────────────────────────────────

    def _iceberg_meta_table_sep(self) -> str:
        return "."

    def _fqn(self, object_name: str) -> str:
        """Return the fully-qualified Spark table name ``database.table``."""
        return f"{self.database}.{object_name}"

    @staticmethod
    def _cast_to_string(values: list) -> list:
        return [f"CAST({v} AS STRING)" for v in values]

    @staticmethod
    def _format_join_condition(
        cols_bks: list, prefix_left: str, prefix_right: str
    ) -> str:
        return " AND ".join(
            f"{prefix_left}.{col} <=> {prefix_right}.{col}" for col in cols_bks
        )

    # ── Private SQL builders ────────────────────────────────────────────────

    @staticmethod
    def _format_case_object(
        name: str,
        is_upd: bool = False,
        upd_key: Optional[str] = None,
        upd_dp_ts_from: Optional[str] = None,
        upd_dp_ts_to: Optional[str] = None,
        upd_dp_is_active: Optional[str] = None,
        upd_dp_is_latest: Optional[str] = None,
        is_upd_2: bool = False,
        upd_key_2: Optional[str] = None,
        upd_dp_ts_from_2: Optional[str] = None,
        upd_dp_ts_to_2: Optional[str] = None,
        upd_dp_is_active_2: Optional[str] = None,
        upd_dp_is_latest_2: Optional[str] = None,
        is_ins: bool = False,
        ins_dp_ts_from: Optional[str] = None,
        ins_dp_ts_to: Optional[str] = None,
        ins_dp_is_active: str = "True",
        ins_dp_is_latest: str = "True",
        is_del: bool = False,
        del_key: Optional[str] = None,
        is_del_2: bool = False,
        del_key_2: Optional[str] = None,
    ) -> str:
        return f"""struct('{name}' AS name, {str(is_upd).lower()} AS is_upd, {f"{upd_key}" if upd_key else 'NULL'} AS upd_key, {f"{upd_dp_ts_from}" if upd_dp_ts_from else 'NULL'} AS upd_dp_ts_from, {f"{upd_dp_ts_to}" if upd_dp_ts_to else 'NULL'} AS upd_dp_ts_to, {f"{str(upd_dp_is_active).lower()}" if upd_dp_is_active is not None else 'NULL'} AS upd_dp_is_active, {f"{str(upd_dp_is_latest).lower()}" if upd_dp_is_latest is not None else 'NULL'} AS upd_dp_is_latest, {str(is_upd_2).lower()} AS is_upd_2, {f"{upd_key_2}" if upd_key_2 else 'NULL'} AS upd_key_2, {f"{upd_dp_ts_from_2}" if upd_dp_ts_from_2 else 'NULL'} AS upd_dp_ts_from_2, {f"{upd_dp_ts_to_2}" if upd_dp_ts_to_2 else 'NULL'} AS upd_dp_ts_to_2, {f"{str(upd_dp_is_active_2).lower()}" if upd_dp_is_active_2 is not None else 'NULL'} AS upd_dp_is_active_2, {f"{str(upd_dp_is_latest_2).lower()}" if upd_dp_is_latest_2 is not None else 'NULL'} AS upd_dp_is_latest_2, {str(is_ins).lower()} AS is_ins, {f"{ins_dp_ts_from}" if ins_dp_ts_from else 'NULL'} AS ins_dp_ts_from, {f"{ins_dp_ts_to}" if ins_dp_ts_to else 'NULL'} AS ins_dp_ts_to, {f"{str(ins_dp_is_active).lower()}" if ins_dp_is_active is not None else 'NULL'} AS ins_dp_is_active, {f"{str(ins_dp_is_latest).lower()}" if ins_dp_is_latest is not None else 'NULL'} AS ins_dp_is_latest, {str(is_del).lower()} AS is_del, {f"{del_key}" if del_key else 'NULL'} AS del_key, {str(is_del_2).lower()} AS is_del_2, {f"{del_key_2}" if del_key_2 else 'NULL'} AS del_key_2)"""

    def _format_cte(
        self,
        cols_bks: list,
        cols_val: list,
        dp_ts: datetime,
    ) -> str:
        fv = self.format_values
        ap = self.add_prefix
        cs = self._cast_to_string

        cols_bks_str = fv(cols_bks)
        prefixed_cols_bks_str = fv(ap(cols_bks, "src"))
        cols_val_str = fv(cols_val)
        prefixed_cols_val_str = fv(ap(cols_val, "src"))
        cast_cols_bks_str = fv(cs(cols_bks))
        cast_cols_val_str = fv(cs(cols_val))
        dp_ts_str = dp_ts.strftime("%Y-%m-%d %H:%M:%S")

        join_src_overlap = self._format_join_condition(cols_bks, "src", "overlap")
        join_src_prev = self._format_join_condition(cols_bks, "src", "prev")
        join_src_next = self._format_join_condition(cols_bks, "src", "next")

        return f"""
    WITH changed_records AS (
        WITH src_records AS (
            SELECT {fv(ap(cols_bks, "t"))}, {fv(ap(cols_val, "t"))}, t.dp_ts_from, t.{self.col_dp_ts_filter}, t.status,
                upper(
                    sha2(
                        concat_ws('||', {fv(cs(ap(cols_bks, "t")))}, {fv(cs(ap(cols_val, "t")))}
                        ), 256
                    )
                ) AS dp_record_hash
            FROM {self.raw_table_fqn()} AS t
            WHERE {self.col_dp_ts_filter} = TIMESTAMP '{dp_ts_str}'
        )
        SELECT
            {prefixed_cols_bks_str},
            {prefixed_cols_val_str},
            src.{self.col_dp_ts}      AS src_dp_ts_from,
            src.dp_record_hash     AS src_dp_record_hash,
            src.{self.col_dp_ts_filter}   AS dp_ts,
            src.status,
            overlap.dp_ts_from                                                                                                      AS overlap_dp_ts_from,
            overlap.dp_ts_to                                                                                                        AS overlap_dp_ts_to,
            overlap.dp_record_id                                                                                                          AS overlap_dp_record_id,
            CASE WHEN overlap.dp_record_hash IS NULL THEN NULL WHEN src.dp_record_hash = overlap.dp_record_hash THEN TRUE ELSE FALSE END     AS overlap_is_same_as_src,
            overlap.dp_is_active                                                                                                    AS overlap_dp_is_active,
            prev.dp_ts_from                                                                                                         AS prev_dp_ts_from,
            prev.dp_ts_to                                                                                                           AS prev_dp_ts_to,
            prev.dp_record_id                                                                                                             AS prev_dp_record_id,
            prev.dp_is_active                                                                                                       AS prev_dp_is_active,
            prev.dp_is_latest                                                                                                       AS prev_dp_is_latest,
            CASE WHEN prev.dp_record_hash IS NULL THEN NULL WHEN src.dp_record_hash = prev.dp_record_hash THEN TRUE ELSE FALSE END           AS prev_is_same_as_src,
            prev.dp_ts_to < src.dp_ts_from - INTERVAL '1' SECOND                                                                    AS prev_with_gap,      
            next.dp_ts_from                                                                                                         AS next_dp_ts_from,
            next.dp_ts_to                                                                                                           AS next_dp_ts_to,
            next.dp_record_id                                                                                                             AS next_dp_record_id,
            next.dp_is_active                                                                                                       AS next_dp_is_active,
            next.dp_is_latest                                                                                                       AS next_dp_is_latest,
            CASE WHEN next.dp_record_hash IS NULL THEN NULL WHEN src.dp_record_hash = next.dp_record_hash THEN TRUE ELSE FALSE END           AS next_is_same_as_src
        FROM src_records AS src
        LEFT JOIN (
            SELECT
                {cols_bks_str},
                dp_record_hash,
                dp_record_id,
                dp_ts_to,
                dp_ts_from,
                dp_is_active,
                dp_is_latest
            FROM {self.scd2_table_fqn()}
        ) overlap
        ON {join_src_overlap}
        AND src.dp_ts_from BETWEEN overlap.dp_ts_from AND overlap.dp_ts_to
        LEFT JOIN (
            SELECT
                dp_record_id,
                {cols_bks_str},
                dp_record_hash,
                dp_ts_from,
                dp_ts_to,
                dp_is_active,
                dp_is_latest
            FROM {self.scd2_table_fqn()}
        ) prev
        ON ({join_src_prev})
        AND (prev.dp_ts_to = src.dp_ts_from - INTERVAL '1' SECOND
            OR (prev.dp_ts_to < src.dp_ts_from AND prev.dp_is_latest = TRUE))
        LEFT JOIN (
            SELECT
                dp_record_id,
                {cols_bks_str},
                dp_record_hash,
                dp_ts_from,
                dp_ts_to,
                dp_is_active,
                dp_is_latest
            FROM {self.scd2_table_fqn()}
            WHERE dp_is_active = TRUE
        ) next
        ON ({join_src_next})
        AND src.dp_ts_from < next.dp_ts_from
    ),
    records_to_process AS (
        SELECT *,
            CASE
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active IS NULL
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_1', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='TIMESTAMP \'9999-12-31 23:59:59\'')}
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = TRUE
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active = TRUE
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_9')}
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = TRUE
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active = FALSE
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_10', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='TIMESTAMP \'9999-12-31 23:59:59\'', upd_dp_is_active='True', upd_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = FALSE
                    AND next_is_same_as_src IS NULL
                    AND (overlap_dp_is_active = TRUE OR overlap_dp_is_active = FALSE)
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_11', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='src_dp_ts_from - INTERVAL \'1\' SECOND', upd_dp_is_active='False', upd_dp_is_latest='False', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='TIMESTAMP \'9999-12-31 23:59:59\'')}
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = TRUE
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active = FALSE
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_12')}
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = FALSE
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active = FALSE
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_13', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='src_dp_ts_from - INTERVAL \'1\' SECOND', upd_dp_is_active='False', upd_dp_is_latest='False', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='overlap_dp_ts_to', ins_dp_is_active='False', ins_dp_is_latest='False')}
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = FALSE
                    AND next_is_same_as_src = TRUE
                    AND overlap_dp_is_active = FALSE
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_14', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='src_dp_ts_from - INTERVAL \'1\' SECOND', upd_dp_is_active='False', upd_dp_is_latest='False', is_upd_2=True, upd_key_2='next_dp_record_id', upd_dp_ts_from_2='src_dp_ts_from', upd_dp_ts_to_2='next_dp_ts_to')}
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = TRUE
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active = FALSE
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_15')}
                WHEN prev_is_same_as_src = FALSE
                    AND overlap_is_same_as_src = FALSE
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active = FALSE
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_16', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='overlap_dp_ts_to', upd_dp_is_active='False', upd_dp_is_latest='False')}                    
                WHEN prev_is_same_as_src = TRUE
                    AND overlap_is_same_as_src = FALSE
                    AND next_is_same_as_src = TRUE
                    AND overlap_dp_is_active = FALSE
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_17', is_upd=True, upd_key='next_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='next_dp_ts_to', is_del=True, del_key='overlap_dp_record_id', is_del_2=True, del_key_2='prev_dp_record_id')}                    
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src = TRUE
                    AND overlap_dp_is_active IS NULL
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_18', is_upd=True, upd_key='next_dp_record_id', upd_dp_ts_from='src_dp_ts_from', upd_dp_ts_to='next_dp_ts_to')}                    
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active IS NULL
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_19', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='next_dp_ts_from - INTERVAL \'1\' SECOND', ins_dp_is_active='False', ins_dp_is_latest='False')}                    
                WHEN prev_is_same_as_src = TRUE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active IS NULL
                    AND prev_with_gap = FALSE
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_20', is_upd=True, upd_key='prev_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='TIMESTAMP \'9999-12-31 23:59:59\'', upd_dp_is_active='True', upd_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src = FALSE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active IS NULL
                    AND prev_with_gap = FALSE
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_21', is_upd=True, upd_key='prev_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='prev_dp_ts_to', upd_dp_is_active='False', upd_dp_is_latest='False', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='TIMESTAMP \'9999-12-31 23:59:59\'', ins_dp_is_active='True', ins_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src = TRUE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active IS NULL
                    AND prev_with_gap = TRUE
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_22', is_upd=True, upd_key='prev_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='prev_dp_ts_to', upd_dp_is_active='False', upd_dp_is_latest='False', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='TIMESTAMP \'9999-12-31 23:59:59\'', ins_dp_is_active='True', ins_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src = FALSE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active IS NULL
                    AND prev_with_gap = TRUE
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_23', is_upd=True, upd_key='prev_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='prev_dp_ts_to', upd_dp_is_active='False', upd_dp_is_latest='False', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='TIMESTAMP \'9999-12-31 23:59:59\'', ins_dp_is_active='True', ins_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src = FALSE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src = TRUE
                    AND overlap_dp_is_active IS NULL
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_24', is_upd=True, upd_key='next_dp_record_id', upd_dp_ts_from='src_dp_ts_from', upd_dp_ts_to='next_dp_ts_to')}           
                WHEN prev_is_same_as_src = TRUE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active IS NULL
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_25', is_upd=True, upd_key='prev_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='next_dp_ts_from - INTERVAL \'1\' SECOND')}           
                WHEN prev_is_same_as_src = TRUE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src = TRUE
                    AND overlap_dp_is_active IS NULL
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_26', is_upd=True, upd_key='prev_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='next_dp_ts_to', upd_dp_is_active='next_dp_is_active', upd_dp_is_latest='next_dp_is_latest', is_del=True, del_key='next_dp_record_id')}           
                WHEN prev_is_same_as_src = FALSE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active IS NULL
                    AND status = 'ACTIVE'
                    THEN {self._format_case_object('CASE_27', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='next_dp_ts_from - INTERVAL \'1\' SECOND', ins_dp_is_active='False', ins_dp_is_latest='False')}           
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = TRUE
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active = TRUE
                    AND status = 'INACTIVE'
                    THEN {self._format_case_object('CASE_30', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='src_dp_ts_from - INTERVAL \'1\' SECOND', upd_dp_is_active='False', upd_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = TRUE
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active = FALSE
                    AND status = 'INACTIVE'
                    THEN {self._format_case_object('CASE_31', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='src_dp_ts_from - INTERVAL \'1\' SECOND', upd_dp_is_active='False', upd_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = FALSE
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active = TRUE
                    AND status = 'INACTIVE'
                    THEN {self._format_case_object('CASE_32', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='src_dp_ts_from - INTERVAL \'1\' SECOND', upd_dp_is_active='False', upd_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src = TRUE
                    AND overlap_is_same_as_src = FALSE
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active = TRUE
                    AND status = 'INACTIVE'
                    THEN {self._format_case_object('CASE_33', is_upd=True, upd_key='prev_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='prev_dp_ts_to', upd_dp_is_active='False', upd_dp_is_latest='True', is_del=True, del_key='overlap_dp_record_id')}                    
            END AS situation                
        FROM changed_records
    ),
    prepared_source AS (
        -- Original records for update (1st update in simple scenarios)
        SELECT
            situation.upd_key                   AS merge_record_id,
            situation.upd_key                   AS dp_record_id,
            {cols_bks_str},
            {cols_val_str},
            src_dp_record_hash                     AS dp_record_hash,
            dp_ts,
            status,
            'UPDATE_VERSION'                    AS operation_type,
            situation.name                      AS case_name,
            situation.upd_dp_ts_from            AS dp_ts_from,
            situation.upd_dp_ts_to              AS dp_ts_to,
            situation.upd_dp_is_active          AS dp_is_active,
            situation.upd_dp_is_latest          AS dp_is_latest
        FROM records_to_process
        WHERE situation.is_upd = TRUE

        UNION ALL

        -- Original records for update (2nd update in complex scenarios)
        SELECT
            situation.upd_key_2                 AS merge_record_id,
            situation.upd_key_2                 AS dp_record_id,
            {cols_bks_str},
            {cols_val_str},
            src_dp_record_hash                     AS dp_record_hash,
            dp_ts,
            status,
            'UPDATE_VERSION'                    AS operation_type,
            situation.name                      AS case_name,            
            situation.upd_dp_ts_from_2          AS dp_ts_from,
            situation.upd_dp_ts_to_2            AS dp_ts_to,
            situation.upd_dp_is_active_2        AS dp_is_active,
            situation.upd_dp_is_latest_2        AS dp_is_latest
        FROM records_to_process
        WHERE situation.is_upd_2 = TRUE

        UNION ALL

        -- Duplicate records for inserts
        SELECT
            NULL AS merge_record_id,
            UUID()                              AS dp_record_id,
            {cols_bks_str},
            {cols_val_str},
            src_dp_record_hash                     AS dp_record_hash,
            dp_ts,
            status,
            'INSERT_NEW_VERSION'                AS operation_type,
            situation.name                      AS case_name,            
            situation.ins_dp_ts_from            AS dp_ts_from,
            situation.ins_dp_ts_to              AS dp_ts_to,
            situation.ins_dp_is_active          AS dp_is_active,
            situation.ins_dp_is_latest          AS dp_is_latest
        FROM records_to_process
        WHERE situation.is_ins = TRUE
        
        UNION ALL

        -- Duplicate records for delete (1st delete in simple scenarios)
        SELECT
            situation.del_key AS merge_record_id,
            situation.del_key AS dp_record_id,
            {cols_bks_str},
            {cols_val_str},
            src_dp_record_hash                     AS dp_record_hash,
            dp_ts,
            status,
            'DELETE_VERSION'                    AS operation_type,
            situation.name                      AS case_name,            
            NULL                                AS dp_ts_from,
            NULL                                AS dp_ts_to,
            NULL                                AS dp_is_active,
            NULL                                AS dp_is_latest
        FROM records_to_process
        WHERE situation.is_del = TRUE

        UNION ALL
        -- Duplicate records for delete (2nd delete in complex scenarios)
        SELECT
            situation.del_key_2 AS merge_record_id,
            situation.del_key_2 AS dp_record_id,
            {cols_bks_str},
            {cols_val_str},
            src_dp_record_hash                     AS dp_record_hash,
            dp_ts,
            status,
            'DELETE_VERSION'                    AS operation_type,
            situation.name                      AS case_name,
            NULL                                AS dp_ts_from,
            NULL                                AS dp_ts_to,
            NULL                                AS dp_is_active,
            NULL                                AS dp_is_latest
        FROM records_to_process
        WHERE situation.is_del_2 = TRUE        
    )
    """

    # ── Public SQL formatters (SCD2Strategy interface) ─────────────────────

    def format_view(
        self,
        dp_ts: datetime,
    ) -> str:
        """Return the CTE + SELECT SQL to be run via spark.sql() and registered
        as a temp view with ``createOrReplaceTempView(scd2_view_name)``."""
        cte = self._format_cte(
            cols_bks=self.cols_bks,
            cols_val=self.cols_val,
            dp_ts=dp_ts,
        )
        return f"""
    CREATE OR REPLACE VIEW {self.scd2_intermediary_table_fqn()} AS
    {cte}
    SELECT *
    FROM prepared_source
    """

    def format_merge(
        self,
        source_view_name: str,
        current_ts: datetime,
    ) -> str:
        fv = self.format_values
        ap = self.add_prefix

        prefixed_cols_val = ap(self.cols_val, "source")
        cols_bks_str = fv(self.cols_bks)
        cols_val_str = fv(self.cols_val)
        source_cols_val_str = fv(prefixed_cols_val)
        current_ts_str = current_ts.strftime("%Y-%m-%d %H:%M:%S")

        return f"""
    MERGE INTO {self.scd2_table_fqn()}  AS target
    USING {source_view_name}            AS source
    ON target.dp_record_id = source.merge_record_id
    WHEN MATCHED
        AND source.operation_type = 'UPDATE_VERSION'
    THEN UPDATE SET
        dp_ts_from = source.dp_ts_from,
        dp_ts_to = source.dp_ts_to,
        dp_is_active = COALESCE(source.dp_is_active, target.dp_is_active),
        dp_is_latest = COALESCE(source.dp_is_latest, target.dp_is_latest),
        dp_replaced_at = TIMESTAMP '{current_ts_str}'
    WHEN MATCHED
        AND source.operation_type = 'DELETE_VERSION'
    THEN DELETE

    WHEN NOT MATCHED
    THEN INSERT (
        dp_record_id,
        {cols_bks_str},
        {cols_val_str},
        {self.col_dp_valid_from},
        {self.col_dp_valid_to},
        dp_is_active,
        dp_is_latest,
        {self.col_dp_created_at},
        {self.col_dp_replaced_at},
        dp_record_hash
    ) VALUES (
        source.dp_record_id,
        {fv(ap(self.cols_bks, "source"))},
        {source_cols_val_str},
        source.dp_ts_from,
        source.dp_ts_to,
        source.dp_is_active,
        source.dp_is_latest,
        TIMESTAMP '{current_ts_str}',
        TIMESTAMP '9999-12-31 23:59:59',
        source.dp_record_hash
    )
    """

    # ── Public operations (SCD2Strategy interface) ─────────────────────────

    def materialize_view(self, scd2_intermediary_table_name: str) -> str:

        stmt = f"DROP TABLE IF EXISTS {self._fqn(scd2_intermediary_table_name)}_mv"
        self.spark.sql(stmt)

        # Drop the S3 folder for the dimension table
        s3_path = f"warehouse/{scd2_intermediary_table_name}_mv"
        #self.delete_s3_location(
        #    s3_client=self.s3_client, bucket="admin-bucket", path=s3_path
        #)

        stmt = f"CREATE TABLE {self._fqn(scd2_intermediary_table_name)}_mv AS SELECT * FROM {self._fqn(scd2_intermediary_table_name)}"

        logger.info(f"{stmt}")
        self.spark.sql(stmt)
        return f"{scd2_intermediary_table_name}_mv"

    def merge_into_scd2_table(
        self,
        dp_ts: datetime,
        current_ts: Optional[datetime] = None,
        show_input_to_merge: bool = False,
        output_file_name: Optional[str] = None,
    ):
        view_stmt = self.format_view(
            dp_ts=dp_ts,
        )

        logger.info(f"Creating SCD2 view: {view_stmt}...")

        df = self.spark.sql(view_stmt)
        df.show(truncate=False)
        df.createOrReplaceTempView(self.scd2_intermediary_table_name)
        logger.info(
            f"SCD2 view {self.scd2_intermediary_table_name} created successfully."
        )

        if self.materialize_data_before_merge:
            # have to materialize the view because of the UUID() function used for generating dp_record_id for inserts - Spark doesn't allow non-deterministic functions in MERGE source!
            self.materialize_view(self.scd2_intermediary_table_name)

        if show_input_to_merge:
            df = self.get_table_data(SCD2Table.INTERMEDIARY, order_by_cols=["merge_record_id"])
            render_table(df, output_file_name=output_file_name, title="Input to Merge")

        if self.perform_merge_op:
            merge_stmt = self.format_merge(
                source_view_name=self.scd2_intermediary_table_fqn() + ("_mv" if self.materialize_data_before_merge else ""),
                current_ts=current_ts,
            )

            logger.info(merge_stmt)
            try:
                result = self.spark.sql(merge_stmt)
                result.show(truncate=False)
            except Exception as e:
                logger.error(f"Error executing merge statement: {e}")
                result = None

    def merge_into_scd2_table_and_return_as_df(
        self,
        dp_ts: datetime,
        current_ts: Optional[datetime] = None,
        show_input_to_merge: bool = False,
        output_file_name: Optional[str] = None,
    ) -> DataFrame:
        self.merge_into_scd2_table(
            dp_ts=dp_ts,
            current_ts=current_ts,
            show_input_to_merge=show_input_to_merge,
            output_file_name=output_file_name,
        )
        return self.spark.table(self.scd2_table_fqn())


    def get_table_data(
        self,
        table: SCD2Table,
        iceberg_meta_tablename: str = None,
        exclude_cols: list = [],
        order_by_cols: list = [],
        for_version: str = None,
    ) -> pd.DataFrame:
        table_fqn = self._resolve_table_fqn(table, iceberg_meta_tablename=iceberg_meta_tablename)
        logger.info(f"Fetching data from table {table_fqn} with iceberg_meta_tablename={iceberg_meta_tablename}, order_by_cols={order_by_cols}, for_version={for_version}...")
        if for_version is not None:
            table_ref = f"{table_fqn} VERSION AS OF {for_version}"
        else:
            table_ref = table_fqn

        if exclude_cols:
            columns = self.spark.table(table_fqn).columns
            column_list = ", ".join(col for col in columns if col not in exclude_cols)
        else:
            column_list = "*"

        order_by_clause = (
            f"ORDER BY {', '.join(f'{col} NULLS LAST' for col in order_by_cols)}"
            if order_by_cols
            else ""
        )

        sql = f"SELECT {column_list} FROM {table_ref} {order_by_clause}"
        return self.spark.sql(sql).toPandas()
