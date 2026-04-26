import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from scd2_strategy import SCD2Strategy, SCD2Table
from util import render_table
from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TrinoSCD2Strategy(SCD2Strategy):
    """SCD2 strategy implementation for Trino / Apache Iceberg via Hive Metastore.

    All SQL is generated in Trino dialect (VARCHAR casts, LATERAL joins,
    IS NOT DISTINCT FROM null-safe equality, to_hex/sha256 hashing, uuid()
    surrogate key generation).

    Args:
        raw_table_name: Unqualified name of the raw staging table that receives
            source records for each load batch (e.g. ``"raw_person"``).
        scd2_table_name: Unqualified name of the SCD2 dimension table
            (e.g. ``"dim_person"``).    

    Usage::

        strategy = TrinoSCD2Strategy(conn, catalog="iceberg_hive", schema="default")
        strategy.create_scd2_table(dim_table_name, ...)
        result, metadata = strategy.merge_into_scd2_table(raw_table_name, ...)
    """

    def __init__(
        self,
        conn,
        catalog: str,
        schema: str,
        raw_table_name: str,
        scd2_table_name: str,
        scd2_intermediary_table_name: str = None,
        cols_bks: Optional[list] = None,
        cols_val: Optional[list] = None,
        use_delta_mode_for_raw_table: bool = False,
        perform_merge_op: bool = True,
        dp_ts_col: str = "dp_ts_version",
        dp_ts_filter_col: str = "dp_ts",
    ):
        super().__init__(
            scd2_intermediary_table_name,
            cols_bks=cols_bks,
            cols_val=cols_val,
            use_delta_mode_for_raw_table=use_delta_mode_for_raw_table,
            perform_merge_op=perform_merge_op,
            dp_ts_col=dp_ts_col,
            dp_ts_filter_col=dp_ts_filter_col,
        )
        self.conn = conn
        self.catalog = catalog
        self.schema = schema
        self.raw_table_name = raw_table_name
        self.scd2_table_name = scd2_table_name        

    # ── Internal helpers ────────────────────────────────────────────────────

    def _iceberg_meta_table_sep(self) -> str:
        return "$"

    def _fqn(self, object_name: str) -> str:
        """Return the fully-qualified Trino table name ``catalog.schema.table``."""
        return f"{self.catalog}.{self.schema}.{object_name}"

    @staticmethod
    def _cast_to_varchar(values: list) -> list:
        return [f"CAST({v} AS VARCHAR)" for v in values]

    @staticmethod
    def _format_join_condition(
        cols_bks: list, prefix_left: str, prefix_right: str
    ) -> str:
        return " AND ".join(
            f"{prefix_left}.{col} IS NOT DISTINCT FROM {prefix_right}.{col}"
            for col in cols_bks
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
        return f"CAST (ROW ('{name}', {str(is_upd).upper()}, {f'{upd_key}' if upd_key else 'NULL'}, {f'{upd_dp_ts_from}' if upd_dp_ts_from else 'NULL'}, {f'{upd_dp_ts_to}' if upd_dp_ts_to else 'NULL'}, {f'{upd_dp_is_active}' if upd_dp_is_active is not None else 'NULL'}, {f'{upd_dp_is_latest}' if upd_dp_is_latest is not None else 'NULL'}, {str(is_upd_2).upper()}, {f'{upd_key_2}' if upd_key_2 else 'NULL'}, {f'{upd_dp_ts_from_2}' if upd_dp_ts_from_2 else 'NULL'}, {f'{upd_dp_ts_to_2}' if upd_dp_ts_to_2 else 'NULL'}, {f'{upd_dp_is_active_2}' if upd_dp_is_active_2 is not None else 'NULL'}, {f'{upd_dp_is_latest_2}' if upd_dp_is_latest_2 is not None else 'NULL'}, {str(is_ins).upper()}, {f'{ins_dp_ts_from}' if ins_dp_ts_from else 'NULL'}, {f'{ins_dp_ts_to}' if ins_dp_ts_to else 'NULL'}, {f'{ins_dp_is_active}' if ins_dp_is_active is not None else 'NULL'}, {f'{ins_dp_is_latest}' if ins_dp_is_latest is not None else 'NULL'}, {str(is_del).upper()}, {f'{del_key}' if del_key else 'NULL'}, {str(is_del_2).upper()}, {f'{del_key_2}' if del_key_2 else 'NULL'}) AS ROW(name VARCHAR, is_upd BOOLEAN, upd_key VARCHAR, upd_dp_ts_from TIMESTAMP, upd_dp_ts_to TIMESTAMP, upd_dp_is_active BOOLEAN, upd_dp_is_latest BOOLEAN, is_upd_2 BOOLEAN, upd_key_2 VARCHAR, upd_dp_ts_from_2 TIMESTAMP, upd_dp_ts_to_2 TIMESTAMP, upd_dp_is_active_2 BOOLEAN, upd_dp_is_latest_2 BOOLEAN, is_ins BOOLEAN, ins_dp_ts_from TIMESTAMP, ins_dp_ts_to TIMESTAMP, ins_dp_is_active BOOLEAN, ins_dp_is_latest BOOLEAN, is_del BOOLEAN, del_key VARCHAR, is_del_2 BOOLEAN, del_key_2 VARCHAR))"

    def _format_cte(
        self,
        cols_bks: list,
        cols_val: list,
        dp_ts: datetime,
    ) -> str:
        fv = self.format_values
        ap = self.add_prefix
        cv = self._cast_to_varchar

        cols_bks_str = fv(cols_bks)
        prefixed_cols_bks_str = fv(ap(cols_bks, "src"))
        cols_val_str = fv(cols_val)
        prefixed_cols_val_str = fv(ap(cols_val, "src"))
        cast_cols_bks_str = fv(cv(cols_bks))
        cast_cols_val_str = fv(cv(cols_val))
        dp_ts_str = dp_ts.strftime("%Y-%m-%d %H:%M:%S")

        join_src_overlap = self._format_join_condition(cols_bks, "src", "overlap")
        join_src_prev = self._format_join_condition(cols_bks, "src", "prev")
        join_src_next = self._format_join_condition(cols_bks, "src", "next")

        return f"""
    WITH changed_records AS (
        WITH src_records AS (
            SELECT *,
                to_hex(
                    sha256(
                        CAST(
                            concat_ws('||', ARRAY[{cast_cols_bks_str}, {cast_cols_val_str}])
                            AS VARBINARY
                        )
                    )
                ) AS dp_record_hash
            FROM {self.raw_table_fqn()}
            WHERE {self.dp_ts_filter_col} = TIMESTAMP '{dp_ts_str}'
        )
        SELECT
            {prefixed_cols_bks_str},
            {prefixed_cols_val_str},
            src.{self.dp_ts_col}      AS src_dp_ts_from,
            src.dp_record_hash     AS src_dp_record_hash,
            src.{self.dp_ts_filter_col}   AS dp_ts,
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
        LEFT JOIN LATERAL (
            SELECT
                {cols_bks_str},
                dp_record_hash,
                dp_record_id,
                dp_ts_to,
                dp_ts_from,
                dp_is_active,
                dp_is_latest
            FROM {self.scd2_table_fqn()}
            WHERE src.dp_ts_from BETWEEN dp_ts_from AND dp_ts_to
        ) overlap
        ON {join_src_overlap}
        LEFT JOIN LATERAL (
            SELECT
                dp_record_id,
                {cols_bks_str},
                dp_record_hash,
                dp_ts_from,
                dp_ts_to,
                dp_is_active,
                dp_is_latest
            FROM {self.scd2_table_fqn()}
            WHERE dp_ts_to = src.dp_ts_from - INTERVAL '1' SECOND       -- previous version if it ends exactly when the new version starts
            OR (dp_ts_to < src.dp_ts_from AND dp_is_latest = TRUE)      -- we are interested in previous version even if it is not ending exactly at the new version, if it is the latest version (so we can update it)
        ) prev
        ON ({join_src_prev})
        LEFT JOIN LATERAL (
            SELECT
                dp_record_id,
                {cols_bks_str},
                dp_record_hash,
                dp_ts_from,
                dp_ts_to,
                dp_is_active,
                dp_is_latest
            FROM {self.scd2_table_fqn()} AS next
            WHERE src.dp_ts_from < next.dp_ts_from
            AND next.dp_is_active = TRUE
        ) next
        ON ({join_src_next})
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
        -- Original records for updates
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
            NULL AS dp_record_id,
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

        -- Duplicate records for deletes
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
        -- Duplicate records for deletes
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
        dp_ts_from,
        dp_ts_to,
        dp_is_active,
        dp_is_latest,
        dp_created_at,
        dp_replaced_at,
        dp_record_hash
    ) VALUES (
        CAST( uuid() AS VARCHAR),
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

        logger.debug(view_stmt)
        self.conn.cursor().execute(view_stmt)
        logger.info("View created successfully.")

        if show_input_to_merge:
            df = self.get_table_data(
                SCD2Table.INTERMEDIARY, order_by_cols=["merge_record_id"]
            )
            render_table(df, output_file_name=output_file_name, title="Input to Merge")

        if self.perform_merge_op:
            merge_stmt = self.format_merge(
                source_view_name=self.scd2_intermediary_table_fqn(),
                current_ts=current_ts,
            )

            logger.info(merge_stmt)

            cursor = self.conn.cursor()
            result = cursor.execute(merge_stmt)
            logger.info(f"Merge result: {result}")        

    def merge_into_scd2_table_and_return_as_df(
        self,
        dp_ts: datetime,
        current_ts: Optional[datetime] = None,
        show_input_to_merge: bool = False,
        output_file_name: Optional[str] = None,
    ) -> DataFrame:
        raise NotImplementedError("This method is not implemented for Trino as we are not in a spark environment.")


    def get_table_data(
        self,
        table: SCD2Table,
        iceberg_meta_tablename: str = None,
        exclude_cols: list = [],
        order_by_cols: list = [],
        for_version: str = None,
    ) -> pd.DataFrame:
        table_fqn = self._resolve_table_fqn(table, iceberg_meta_tablename=iceberg_meta_tablename)
        cursor = self.conn.cursor()

        if exclude_cols:
            cursor.execute(f"SELECT * FROM {table_fqn} LIMIT 0")
            columns = [desc[0] for desc in cursor.description]
            column_list = ", ".join(col for col in columns if col not in exclude_cols)
        else:
            column_list = "*"

        order_by_clause = (
            f"ORDER BY {', '.join(f'{col} NULLS LAST' for col in order_by_cols)}"
            if order_by_cols
            else ""
        )

        if for_version is not None:
            table_fqn = f"{table_fqn} FOR VERSION AS OF {for_version}"

        sql = f"SELECT {column_list} FROM {table_fqn} {order_by_clause}"
        return pd.read_sql_query(sql, self.conn)
