import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from .scd2_strategy import SCD2Strategy, SCD2Table
from .util import render_table
from .constants import MAX_TS
from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TrinoSCD2Strategy(SCD2Strategy):
    """SCD2 strategy implementation for Trino / Apache Iceberg via Hive Metastore.

    All SQL is generated in Trino dialect (VARCHAR casts, LATERAL joins,
    IS NOT DISTINCT FROM null-safe equality, to_hex/sha256 hashing, uuid()
    surrogate key generation).

    Args:
        source_table_name: Unqualified name of the raw staging table that receives
            source records for each load batch (e.g. ``"raw_person"``).
        scd2_table_name: Unqualified name of the SCD2 dimension table
            (e.g. ``"dim_person"``).    

    Usage::

        strategy = TrinoSCD2Strategy(conn, catalog="iceberg_hive", schema="default")
        strategy.create_scd2_table(dim_table_name, ...)
        result, metadata = strategy.merge_into_scd2_table(source_table_name, ...)
    """

    def __init__(
        self,
        conn,
        catalog: str,
        schema: str,
        source_table_name: str,
        scd2_table_name: str,
        scd2_intermediary_table_name: str = None,
        cols_bks: Optional[list] = None,
        cols_val: Optional[list] = None,
        use_logical_delete_for_source_table: bool = False,
        logical_delete_expression: Optional[str] = None,
        materialize_data_before_merge: bool = False,
        check_physical_delete_against_source_table: bool = True,
        perform_merge_op: bool = True,
        perform_record_hash_update: bool = False,
        col_dp_valid_from: str = "dp_from_ts",
        col_dp_valid_to: str = "dp_to_ts",
        col_dp_created_at: str = "dp_created_at",
        col_dp_replaced_at: str = "dp_replaced_at",      
        col_dp_record_hash: str = "dp_record_hash",
        col_dp_record_id : str = "dp_record_id",         
        col_dp_ts: str = "dp_ts_version",
        col_dp_ts_filter: str = "dp_ts",
    ):
        """
        Args:
            spark: Active SparkSession.
            catalog: Spark catalog database that owns all tables (e.g. ``"default"``).
            schema: Spark schema that owns all tables (e.g. ``"public"``).
            source_table_name: Catalog name of the source/staging table that holds
                incoming raw records (e.g. ``"raw_person"``).
            scd2_table_name: Catalog name of the SCD2 target dimension table
                (e.g. ``"dim_person"``).
            scd2_intermediary_table_name: Name for the temporary staging view / table
                built before the MERGE. Defaults to ``"{scd2_table_name}_temp"``. Only needed if ``materialize_data_before_merge=True``.
            cols_bks: Business-key column names that uniquely identify an entity. An array of column names whose values uniquely identify an entity (e.g. ``["person_id"]``).
            cols_val: Value/attribute column names whose changes trigger new SCD2
                versions (e.g. ``["first_name", "last_name", "city"]``).
            use_logical_delete_for_source_table: When ``True`` the source table
                contains an explicit deleted/inactive flag; ``logical_delete_expression``
                is used to derive ``dp_del_flag``.  When ``False`` (default) deletes
                are detected by comparing the current batch against the previous one
                (physical-delete / full-snapshot mode).
            logical_delete_expression: SQL expression that evaluates to ``True`` for
                logically-deleted rows (e.g. ``"is_deleted = true"``).  Required when
                ``use_logical_delete_for_source_table=True``.
            materialize_data_before_merge: When ``True`` the intermediary staging view
                is written to a physical Iceberg table (``{intermediary}_mv``) before
                the MERGE runs.  Use this if your Spark version does not support the
                cached temp-view path.  Defaults to ``False``.
            check_physical_delete_against_source_table: When ``True`` (default) missing
                entities are detected by comparing the current batch with the previous
                batch in the *source* table.  When ``False`` the currently-active rows
                in the *SCD2 table* are used as the reference for deletes instead.
            perform_merge_op: Set to ``False`` to skip the ``MERGE INTO`` statement
                (useful for inspecting the staging data without modifying the target).
            perform_record_hash_update: When ``True``, all NULL values in col_dp_record_hash columns will be updated before the SCD2 merge operation is performed.  Defaults to ``False``.
            col_dp_valid_from: Column name for the validity-start timestamp in the SCD2
                table.  Defaults to ``"dp_from_ts"``.
            col_dp_valid_to: Column name for the validity-end timestamp in the SCD2
                table.  Defaults to ``"dp_to_ts"``.
            col_dp_created_at: Column name recording when the SCD2 row was first
                inserted.  Defaults to ``"dp_created_at"``.
            col_dp_replaced_at: Column name recording when the SCD2 row was last
                updated or superseded.  Defaults to ``"dp_replaced_at"``.
            col_dp_record_hash: Name of the column in the SCD2 table that stores the record hash (e.g. ``"dp_record_hash"``).  Defaults to ``"dp_record_hash"``.
            col_dp_record_id: Name of the column in the SCD2 table that stores the unique ID of each record (e.g. ``"dp_record_id"``).  Defaults to ``"dp_record_id"``.
            col_dp_ts: Column name in the source table that holds the record's own
                version timestamp (used as ``dp_from_ts`` for new SCD2 rows).
                Defaults to ``"dp_ts_version"``.
            col_dp_ts_filter: Column name used to filter the source table to a single
                batch/snapshot (e.g. ``"dp_ts"``).  Set to ``None`` to disable
                filtering and process the entire source table.  Defaults to ``"dp_ts"``.
        """        
        super().__init__(
            scd2_intermediary_table_name=(
                scd2_intermediary_table_name or f"{scd2_table_name}_temp"
            ),
            cols_bks=cols_bks,
            cols_val=cols_val,
            use_logical_delete_for_source_table=use_logical_delete_for_source_table,
            logical_delete_expression=logical_delete_expression,
            materialize_data_before_merge=materialize_data_before_merge,
            check_physical_delete_against_source_table=check_physical_delete_against_source_table,
            perform_merge_op=perform_merge_op,
            perform_record_hash_update=perform_record_hash_update,
            col_dp_valid_from=col_dp_valid_from,
            col_dp_valid_to=col_dp_valid_to,
            col_dp_created_at=col_dp_created_at,
            col_dp_replaced_at=col_dp_replaced_at,
            col_dp_record_hash=col_dp_record_hash,
            col_dp_record_id=col_dp_record_id,
            col_dp_ts=col_dp_ts,
            col_dp_ts_filter=col_dp_ts_filter,
        )
        self.conn = conn
        self.catalog = catalog
        self.schema = schema
        self.source_table_name = source_table_name
        self.scd2_table_name = scd2_table_name        

    # ── Internal helpers ────────────────────────────────────────────────────

    def _iceberg_meta_table_sep(self) -> str:
        return "$"

    def _fqn(self, object_name: str) -> str:
        """Return the fully-qualified Trino table name ``catalog.schema.table``."""
        return f"{self.catalog}.{self.schema}.{object_name}"

    @staticmethod
    def _cast_to_varchar(value: str) -> str:
        return f"CAST({value} AS VARCHAR)"

    @staticmethod
    def _cast_to_json(value: str) -> str:
        return f"json_format(CAST({value} AS JSON))"

    @staticmethod
    def _is_complex_type(datatype: str) -> bool:
        dt = datatype.strip().lower()
        return dt.startswith(("array"))

    @staticmethod
    def _split_inner(datatype: str, prefix_len: int) -> list:
        """Split the inner content of a parametric type, respecting nested parens."""
        inner = datatype.strip()[prefix_len:-1].strip()
        parts = []
        depth = 0
        current = ""
        for ch in inner:
            if ch == "(":
                depth += 1
                current += ch
            elif ch == ")":
                depth -= 1
                current += ch
            elif ch == "," and depth == 0:
                parts.append(current.strip())
                current = ""
            else:
                current += ch
        if current.strip():
            parts.append(current.strip())
        return parts

    @staticmethod
    def _parse_fields(datatype: str) -> list:
        """Parse 'row(field1 type1, field2 type2, ...)' into [(field1, type1), ...]."""
        parts = TrinoSCD2Strategy._split_inner(datatype, prefix_len=4)  # strip "row("
        return [tuple(p.split(None, 1)) for p in parts if p]

    @staticmethod
    def _cast_values(values: list, cols_with_type: dict) -> list:
        result = []
        for v in values:
            dt = cols_with_type.get(v, "").strip().lower()
            if TrinoSCD2Strategy._is_complex_type(dt):
                result.append(TrinoSCD2Strategy._cast_to_varchar(TrinoSCD2Strategy._cast_to_json(v)))
            elif dt.startswith("map("):
                result.append(TrinoSCD2Strategy._cast_to_varchar(TrinoSCD2Strategy._cast_to_json(f"map_from_entries(array_sort(map_entries({v}), (a, b) -> IF(a[1] < b[1], -1, 1)))")))
            elif dt.startswith("row("):
                fields = TrinoSCD2Strategy._parse_fields(dt)
                sub_values = [f"{v}.{fname}" for fname, _ in fields]
                sub_cols_with_type = {f"{v}.{fname}": ftype for fname, ftype in fields}
                result.extend(TrinoSCD2Strategy._cast_values(sub_values, sub_cols_with_type))
            elif dt in ("double", "real"):
                result.append(TrinoSCD2Strategy._cast_to_varchar(f"CAST({v} AS DECIMAL(18,6))"))
            elif dt in ("timestamp(6)", "timestamp(6) with time zone"):
                result.append(TrinoSCD2Strategy._cast_to_varchar(f"date_format({v}, '%Y-%m-%d %H:%i:%s')"))
            else:
                result.append(TrinoSCD2Strategy._cast_to_varchar(v))
        return result

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

    def _cols_with_type(self, table_name: str) -> dict:
        sql = f"""
            SELECT LOWER(column_name), LOWER(data_type)
            FROM {self.catalog}.information_schema.columns
            WHERE table_schema = '{self.schema}'
              AND table_name = '{table_name}'
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return {row[0]: row[1] for row in cursor.fetchall()}
    
    @staticmethod
    def _format_hash_expr(cols_bks: list, cols_val: list, cols_with_type: dict) -> str:
        """
        Format a hash expression that concatenates and hashes all BK and value columns.
            Complex types (ARRAY, MAP, ROW) are first converted to JSON strings with sorted keys to ensure consistent hashing.
            Timestamps are cast to a common format to avoid issues with different precisions or time zones.
            All values are cast to VARCHAR before concatenation.
            The final hash is computed as to_hex(sha256(...)).
        """
        cast_cols_bks_str = TrinoSCD2Strategy.format_values(TrinoSCD2Strategy._cast_values(cols_bks, cols_with_type))
        cast_cols_val_str = TrinoSCD2Strategy.format_values(TrinoSCD2Strategy._cast_values(cols_val, cols_with_type))
        return f"to_hex(sha256(CAST(concat_ws('||', ARRAY[{cast_cols_bks_str}, {cast_cols_val_str}]) AS VARBINARY)))"
    
    def _format_cte(
        self,
        cols_bks: list,
        cols_val: list,
        dp_ts: datetime,
    ) -> str:
        fv = self.format_values
        ap = self.add_prefix
#        cv = self._cast_values

        cols_bks_str = fv(cols_bks)
        prefixed_cols_bks_str = fv(ap(cols_bks, "src"))
        cols_val_str = fv(cols_val)
        prefixed_cols_val_str = fv(ap(cols_val, "src"))
        dp_ts_str = dp_ts.strftime("%Y-%m-%d %H:%M:%S")

        join_curr_prev = self._format_join_condition(cols_bks, "curr", "prev")
        join_src_overlap = self._format_join_condition(cols_bks, "src", "overlap")
        join_src_prev = self._format_join_condition(cols_bks, "src", "prev")
        join_src_next = self._format_join_condition(cols_bks, "src", "next")
        where_curr_is_null = " AND ".join(f"curr.{col} IS NULL" for col in cols_bks)

        cols_with_type = self._cols_with_type(self.source_table_name)
        hash_expr = self._format_hash_expr(self.cols_bks, self.cols_val, cols_with_type)

        dp_ts_filter_expr = f"WHERE {self.col_dp_ts_filter} = TIMESTAMP '{dp_ts_str}'" if self.col_dp_ts_filter and self.col_dp_ts_filter is not None else ""

        if (self.check_physical_delete_against_source_table):
            dp_ts_filter_prev_expr = f"WHERE {self.col_dp_ts_filter} = (SELECT MAX({self.col_dp_ts_filter}) FROM {self.source_table_fqn()} WHERE {self.col_dp_ts_filter} < TIMESTAMP '{dp_ts_str}')" if self.col_dp_ts_filter and self.col_dp_ts_filter is not None else ""
        else:
            dp_ts_filter_prev_expr = f"WHERE TIMESTAMP '{dp_ts_str}' BETWEEN {self.col_dp_valid_from} AND {self.col_dp_valid_to}"

        if (self.use_logical_delete_for_source_table):
            dp_del_flag_expr = f"CASE WHEN {self.logical_delete_expression} THEN 'INACTIVE' ELSE 'ACTIVE' END AS dp_del_flag"
        else:    
            dp_del_flag_expr = "'N.A.' AS dp_del_flag"

        SRC_DATA_DELTA_CTE = f"""
            src_records AS (
                SELECT {fv(ap(cols_bks, "t"))}, {fv(ap(cols_val, "t"))}, t.{self.col_dp_ts}, t.{self.col_dp_ts_filter},
                        {hash_expr} AS {self.col_dp_record_hash},
                    {dp_del_flag_expr}
                FROM {self.source_table_fqn()} AS t
                {dp_ts_filter_expr}
            )
        """

        PREV_DATA_FROM_SOURCE_CTE = f"""
            prev_src_records AS (
                SELECT {fv(ap(cols_bks, "t"))}, {fv(ap(cols_val, "t"))}, t.{self.col_dp_ts}, t.{self.col_dp_ts_filter},
                        {hash_expr} AS {self.col_dp_record_hash},
                    {dp_del_flag_expr}
                FROM {self.source_table_fqn()} AS t
                {dp_ts_filter_prev_expr} 
            )
        """

        PREV_DATA_FROM_SCD2_CTE = f"""
            prev_src_records AS (
                SELECT {fv(ap(cols_bks, "t"))}, {fv(ap(cols_val, "t"))}, NULL AS dp_ts_from, NULL AS dp_loaded_at,
                    {self.col_dp_record_hash},
                    {dp_del_flag_expr}
                FROM {self.scd2_table_fqn()} AS t
                {dp_ts_filter_prev_expr} 
            )
        """

        if self.check_physical_delete_against_source_table:
            PREV_DATA_CTE = PREV_DATA_FROM_SOURCE_CTE
        else:
            PREV_DATA_CTE = PREV_DATA_FROM_SCD2_CTE

        SRC_DATA_FULL_CTE = f"""
            src_curr_records AS (
                SELECT {fv(ap(cols_bks, "t"))}, {fv(ap(cols_val, "t"))}, t.{self.col_dp_ts}, t.{self.col_dp_ts_filter},
                        {hash_expr} AS {self.col_dp_record_hash},
                    {dp_del_flag_expr}
                FROM {self.source_table_fqn()} AS t
                {dp_ts_filter_expr} 
            ),       
            {PREV_DATA_CTE},
            src_records AS (
                SELECT
                    {fv(ap(cols_bks, "curr"))}, {fv(ap(cols_val, "curr"))}, curr.{self.col_dp_ts}, curr.{self.col_dp_ts_filter},
                    curr.{self.col_dp_record_hash},
                    'ACTIVE'              AS dp_del_flag
                FROM src_curr_records    curr
                UNION ALL
                SELECT
                    {fv(ap(cols_bks, "prev"))}, {fv(ap(cols_val, "prev"))}, TIMESTAMP '{dp_ts_str}', TIMESTAMP '{dp_ts_str}',
                    prev.{self.col_dp_record_hash},
                    'INACTIVE'            AS dp_del_flag
                FROM prev_src_records prev
                LEFT JOIN src_curr_records curr
                ON ({join_curr_prev})
                WHERE {where_curr_is_null}
            )            
        """

        src_data_cte = SRC_DATA_DELTA_CTE if self.use_logical_delete_for_source_table else SRC_DATA_FULL_CTE

        return f"""
    WITH changed_records AS (
        WITH 
            {src_data_cte}
        SELECT
            {prefixed_cols_bks_str},
            {prefixed_cols_val_str},
            src.{self.col_dp_ts}      AS src_dp_ts_from,
            src.{self.col_dp_record_hash}     AS src_{self.col_dp_record_hash},
            src.dp_del_flag,
            overlap.dp_ts_from                                                                                                      AS overlap_dp_ts_from,
            overlap.dp_ts_to                                                                                                        AS overlap_dp_ts_to,
            overlap.{self.col_dp_record_id}                                                                                                          AS overlap_{self.col_dp_record_id},
            CASE WHEN overlap.{self.col_dp_record_hash} IS NULL THEN NULL WHEN src.{self.col_dp_record_hash} = overlap.{self.col_dp_record_hash} THEN TRUE ELSE FALSE END     AS overlap_is_same_as_src,
            overlap.dp_is_active                                                                                                    AS overlap_dp_is_active,
            prev.dp_ts_from                                                                                                         AS prev_dp_ts_from,
            prev.dp_ts_to                                                                                                           AS prev_dp_ts_to,
            prev.{self.col_dp_record_id}                                                                                                             AS prev_{self.col_dp_record_id},
            prev.dp_is_active                                                                                                       AS prev_dp_is_active,
            prev.dp_is_latest                                                                                                       AS prev_dp_is_latest,
            CASE WHEN prev.{self.col_dp_record_hash} IS NULL THEN NULL WHEN src.{self.col_dp_record_hash} = prev.{self.col_dp_record_hash} THEN TRUE ELSE FALSE END           AS prev_is_same_as_src,
            prev.dp_ts_to < src.dp_ts_from - INTERVAL '1' SECOND                                                                    AS prev_with_gap,      
            next.dp_ts_from                                                                                                         AS next_dp_ts_from,
            next.dp_ts_to                                                                                                           AS next_dp_ts_to,
            next.{self.col_dp_record_id}                                                                                                             AS next_{self.col_dp_record_id},
            next.dp_is_active                                                                                                       AS next_dp_is_active,
            next.dp_is_latest                                                                                                       AS next_dp_is_latest,
            CASE WHEN next.{self.col_dp_record_hash} IS NULL THEN NULL WHEN src.{self.col_dp_record_hash} = next.{self.col_dp_record_hash} THEN TRUE ELSE FALSE END           AS next_is_same_as_src
        FROM src_records AS src
        LEFT JOIN LATERAL (
            SELECT
                {cols_bks_str},
                {self.col_dp_record_hash},
                {self.col_dp_record_id},
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
                {self.col_dp_record_id},
                {cols_bks_str},
                {self.col_dp_record_hash},
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
                {self.col_dp_record_id},
                {cols_bks_str},
                {self.col_dp_record_hash},
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
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_1', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='TIMESTAMP \'9999-12-31 23:59:59\'')}
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = TRUE
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active = TRUE
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_9')}
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = TRUE
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active = FALSE
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_10', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='TIMESTAMP \'9999-12-31 23:59:59\'', upd_dp_is_active='True', upd_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = FALSE
                    AND next_is_same_as_src IS NULL
                    AND (overlap_dp_is_active = TRUE OR overlap_dp_is_active = FALSE)
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_11', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='src_dp_ts_from - INTERVAL \'1\' SECOND', upd_dp_is_active='False', upd_dp_is_latest='False', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='TIMESTAMP \'9999-12-31 23:59:59\'')}
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = TRUE
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active = FALSE
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_12')}
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = FALSE
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active = FALSE
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_13', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='src_dp_ts_from - INTERVAL \'1\' SECOND', upd_dp_is_active='False', upd_dp_is_latest='False', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='overlap_dp_ts_to', ins_dp_is_active='False', ins_dp_is_latest='False')}
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = FALSE
                    AND next_is_same_as_src = TRUE
                    AND overlap_dp_is_active = FALSE
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_14', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='src_dp_ts_from - INTERVAL \'1\' SECOND', upd_dp_is_active='False', upd_dp_is_latest='False', is_upd_2=True, upd_key_2='next_dp_record_id', upd_dp_ts_from_2='src_dp_ts_from', upd_dp_ts_to_2='next_dp_ts_to')}
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = TRUE
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active = FALSE
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_15')}
                WHEN prev_is_same_as_src = FALSE
                    AND overlap_is_same_as_src = FALSE
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active = FALSE
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_16', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='overlap_dp_ts_to', upd_dp_is_active='False', upd_dp_is_latest='False')}                    
                WHEN prev_is_same_as_src = TRUE
                    AND overlap_is_same_as_src = FALSE
                    AND next_is_same_as_src = TRUE
                    AND overlap_dp_is_active = FALSE
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_17', is_upd=True, upd_key='next_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='next_dp_ts_to', is_del=True, del_key='overlap_dp_record_id', is_del_2=True, del_key_2='prev_dp_record_id')}                    
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src = TRUE
                    AND overlap_dp_is_active IS NULL
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_18', is_upd=True, upd_key='next_dp_record_id', upd_dp_ts_from='src_dp_ts_from', upd_dp_ts_to='next_dp_ts_to')}                    
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active IS NULL
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_19', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='next_dp_ts_from - INTERVAL \'1\' SECOND', ins_dp_is_active='False', ins_dp_is_latest='False')}                    
                WHEN prev_is_same_as_src = TRUE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active IS NULL
                    AND prev_with_gap = FALSE
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_20', is_upd=True, upd_key='prev_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='TIMESTAMP \'9999-12-31 23:59:59\'', upd_dp_is_active='True', upd_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src = FALSE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active IS NULL
                    AND prev_with_gap = FALSE
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_21', is_upd=True, upd_key='prev_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='prev_dp_ts_to', upd_dp_is_active='False', upd_dp_is_latest='False', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='TIMESTAMP \'9999-12-31 23:59:59\'', ins_dp_is_active='True', ins_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src = TRUE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active IS NULL
                    AND prev_with_gap = TRUE
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_22', is_upd=True, upd_key='prev_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='prev_dp_ts_to', upd_dp_is_active='False', upd_dp_is_latest='False', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='TIMESTAMP \'9999-12-31 23:59:59\'', ins_dp_is_active='True', ins_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src = FALSE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active IS NULL
                    AND prev_with_gap = TRUE
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_23', is_upd=True, upd_key='prev_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='prev_dp_ts_to', upd_dp_is_active='False', upd_dp_is_latest='False', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='TIMESTAMP \'9999-12-31 23:59:59\'', ins_dp_is_active='True', ins_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src = FALSE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src = TRUE
                    AND overlap_dp_is_active IS NULL
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_24', is_upd=True, upd_key='next_dp_record_id', upd_dp_ts_from='src_dp_ts_from', upd_dp_ts_to='next_dp_ts_to')}           
                WHEN prev_is_same_as_src = TRUE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active IS NULL
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_25', is_upd=True, upd_key='prev_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='next_dp_ts_from - INTERVAL \'1\' SECOND')}           
                WHEN prev_is_same_as_src = TRUE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src = TRUE
                    AND overlap_dp_is_active IS NULL
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_26', is_upd=True, upd_key='prev_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='next_dp_ts_to', upd_dp_is_active='next_dp_is_active', upd_dp_is_latest='next_dp_is_latest', is_del=True, del_key='next_dp_record_id')}           
                WHEN prev_is_same_as_src = FALSE
                    AND overlap_is_same_as_src IS NULL
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active IS NULL
                    AND dp_del_flag = 'ACTIVE'
                    THEN {self._format_case_object('CASE_27', is_ins=True, ins_dp_ts_from='src_dp_ts_from', ins_dp_ts_to='next_dp_ts_from - INTERVAL \'1\' SECOND', ins_dp_is_active='False', ins_dp_is_latest='False')}           
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = TRUE
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active = TRUE
                    AND dp_del_flag = 'INACTIVE'
                    THEN {self._format_case_object('CASE_30', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='src_dp_ts_from - INTERVAL \'1\' SECOND', upd_dp_is_active='False', upd_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = TRUE
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active = FALSE
                    AND dp_del_flag = 'INACTIVE'
                    THEN {self._format_case_object('CASE_31', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='src_dp_ts_from - INTERVAL \'1\' SECOND', upd_dp_is_active='False', upd_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src IS NULL
                    AND overlap_is_same_as_src = FALSE
                    AND next_is_same_as_src IS NULL
                    AND overlap_dp_is_active = TRUE
                    AND dp_del_flag = 'INACTIVE'
                    THEN {self._format_case_object('CASE_32', is_upd=True, upd_key='overlap_dp_record_id', upd_dp_ts_from='overlap_dp_ts_from', upd_dp_ts_to='src_dp_ts_from - INTERVAL \'1\' SECOND', upd_dp_is_active='False', upd_dp_is_latest='True')}                    
                WHEN prev_is_same_as_src = TRUE
                    AND overlap_is_same_as_src = FALSE
                    AND next_is_same_as_src = FALSE
                    AND overlap_dp_is_active = TRUE
                    AND dp_del_flag = 'INACTIVE'
                    THEN {self._format_case_object('CASE_33', is_upd=True, upd_key='prev_dp_record_id', upd_dp_ts_from='prev_dp_ts_from', upd_dp_ts_to='prev_dp_ts_to', upd_dp_is_active='False', upd_dp_is_latest='True', is_del=True, del_key='overlap_dp_record_id')}                    
            END AS situation                
        FROM changed_records
    ),
    prepared_source AS (
        -- Original records for updates
        SELECT
            situation.upd_key                   AS merge_record_id,
            situation.upd_key                   AS {self.col_dp_record_id},
            {cols_bks_str},
            {cols_val_str},
            src_{self.col_dp_record_hash}                     AS {self.col_dp_record_hash},
            dp_del_flag,
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
            situation.upd_key_2                 AS {self.col_dp_record_id},
            {cols_bks_str},
            {cols_val_str},
            src_{self.col_dp_record_hash}                     AS {self.col_dp_record_hash},
            dp_del_flag,
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
            NULL AS {self.col_dp_record_id},
            {cols_bks_str},
            {cols_val_str},
            src_{self.col_dp_record_hash}                     AS {self.col_dp_record_hash},
            dp_del_flag,
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
            situation.del_key AS {self.col_dp_record_id},
            {cols_bks_str},
            {cols_val_str},
            src_{self.col_dp_record_hash}                     AS {self.col_dp_record_hash},
            dp_del_flag,
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
            situation.del_key_2 AS {self.col_dp_record_id},
            {cols_bks_str},
            {cols_val_str},
            src_{self.col_dp_record_hash}                     AS {self.col_dp_record_hash},
            dp_del_flag,
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
    ON target.{self.col_dp_record_id} = source.merge_record_id
    WHEN MATCHED
        AND source.operation_type = 'UPDATE_VERSION'
    THEN UPDATE SET
        {self.col_dp_valid_from} = source.{self.col_dp_valid_from},
        {self.col_dp_valid_to} = source.{self.col_dp_valid_to},
        dp_is_active = COALESCE(source.dp_is_active, target.dp_is_active),
        dp_is_latest = COALESCE(source.dp_is_latest, target.dp_is_latest),
        {self.col_dp_replaced_at} = TIMESTAMP '{current_ts_str}'
    WHEN MATCHED
        AND source.operation_type = 'DELETE_VERSION'
    THEN DELETE

    WHEN NOT MATCHED
    THEN INSERT (
        {self.col_dp_record_id},
        {cols_bks_str},
        {cols_val_str},
        {self.col_dp_valid_from},
        {self.col_dp_valid_to},
        dp_is_active,
        dp_is_latest,
        {self.col_dp_created_at},
        {self.col_dp_replaced_at},
        {self.col_dp_record_hash}
    ) VALUES (
        CAST( uuid() AS VARCHAR),
        {fv(ap(self.cols_bks, "source"))},
        {source_cols_val_str},
        source.{self.col_dp_valid_from},
        source.{self.col_dp_valid_to},
        source.dp_is_active,
        source.dp_is_latest,
        TIMESTAMP '{current_ts_str}',
        TIMESTAMP '{MAX_TS}',
        source.{self.col_dp_record_hash}
    )
    """

    # ── Public operations (SCD2Strategy interface) ─────────────────────────

    def materialize_view(self, scd2_intermediary_table_name: str) -> str:

        stmt = f"DROP TABLE IF EXISTS {self._fqn(scd2_intermediary_table_name)}_mv"
        cursor = self.conn.cursor()
        cursor.execute(stmt)        

        # Drop the S3 folder for the dimension table
        #s3_path = f"warehouse/{scd2_intermediary_table_name}_mv"
        #self.delete_s3_location(
        #    s3_client=self.s3_client, bucket="admin-bucket", path=s3_path
        #)

        stmt = f"CREATE TABLE {self._fqn(scd2_intermediary_table_name)}_mv AS SELECT * FROM {self._fqn(scd2_intermediary_table_name)}"

        logger.info(f"{stmt}")
        cursor.execute(stmt)      
        return f"{scd2_intermediary_table_name}_mv"
    
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

    def fill_empty_record_hash_vals_in_scd2_table(self):
        cols_with_type = self._cols_with_type(self.source_table_name)
        hash_expr = self._format_hash_expr(self.cols_bks, self.cols_val, cols_with_type)

        # This is a safety measure to ensure that we don't have any NULL values in the record hash column, which could cause issues with the merge logic. We can set it to a constant value since we are only interested in detecting changes, and any change from or to NULL will be detected as a change.
        stmt = f"""
            UPDATE {self.scd2_table_fqn()}
            SET {self.col_dp_record_hash} = {hash_expr}
            WHERE {self.col_dp_record_hash} IS NULL
        """
        cursor = self.conn.cursor()
        cursor.execute(stmt)    

    def merge_into_scd2_table(
        self,
        dp_ts: datetime,
        current_ts: Optional[datetime] = None,
        show_input_to_merge: bool = False,
        output_file_name: Optional[str] = None,
    ):
        if self.perform_record_hash_update:
            logger.info("Filling empty record hash values in SCD2 table before merge...")
            self.fill_empty_record_hash_vals_in_scd2_table()
            logger.info("Empty record hash values in SCD2 table filled successfully.")

        view_stmt = self.format_view(
            dp_ts=dp_ts,
        )

        print(view_stmt)
        logger.debug(f"view_stmt which defines the input data for the MERGE statement: {view_stmt}")
        self.conn.cursor().execute(view_stmt)
        logger.info("View created successfully.")

        if self.materialize_data_before_merge:
            self.materialize_view(self.scd2_intermediary_table_name)

        if show_input_to_merge:
            df = self.get_table_data(
                SCD2Table.INTERMEDIARY, order_by_cols=["merge_record_id"]
            )
            render_table(df, output_file_name=output_file_name, title="Input to Merge")

        if self.perform_merge_op:
            merge_stmt = self.format_merge(
#                source_view_name=self.scd2_intermediary_table_fqn(),
                source_view_name=(self.scd2_intermediary_table_fqn() + "_mv") if self.materialize_data_before_merge else self.scd2_intermediary_table_fqn(),
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
