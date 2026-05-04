import logging
from datetime import datetime
from functools import reduce
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from .scd2_spark import SparkSCD2Strategy
from .scd2_strategy import SCD2Table
from .util import render_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_MAX_TS = "TIMESTAMP '9999-12-31 23:59:59'"
_ONE_SEC = "INTERVAL '1' SECOND"


class PySparkSCD2Strategy(SparkSCD2Strategy):
    """SCD2 strategy using the PySpark DataFrame API for staging computation.

    Identical in behaviour to :class:`SparkSCD2Strategy` but replaces the SQL
    CTE / CREATE VIEW logic with PySpark DataFrame transformations.  Only the
    final ``MERGE INTO`` statement stays as Spark SQL (Spark has no native
    DataFrame-level MERGE API).

    Usage::

        strategy = PySparkSCD2Strategy(spark, s3_client, database="default",
                                        raw_table_name="raw_person",
                                        scd2_table_name="dim_person")
        strategy.create_scd2_table(...)
        result, _ = strategy.merge_into_scd2_table(...)
    """

    def __init__(
        self,
        spark,
        database: str,
        raw_table_name: str,
        raw_table_df: DataFrame,
        scd2_table_name: str,
        scd2_intermediary_table_name: str = None,
        cols_bks: Optional[list] = None,
        cols_val: Optional[list] = None,
        use_delta_mode_for_raw_table: bool = False,
        delta_mode_delete_expression: Optional[str] = None,
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
            spark=spark,
            database=database,
            raw_table_name=raw_table_name,
            scd2_table_name=scd2_table_name,
            scd2_intermediary_table_name=scd2_intermediary_table_name,
            cols_bks=cols_bks,
            cols_val=cols_val,
            use_delta_mode_for_raw_table=use_delta_mode_for_raw_table,
            delta_mode_delete_expression=delta_mode_delete_expression,
            materialize_data_before_merge=materialize_data_before_merge,
            perform_merge_op=perform_merge_op,
            col_dp_valid_from=col_dp_valid_from,
            col_dp_valid_to=col_dp_valid_to,
            col_dp_created_at=col_dp_created_at,
            col_dp_replaced_at=col_dp_replaced_at,
            col_dp_ts=col_dp_ts,
            col_dp_ts_filter=col_dp_ts_filter,
        ) 

        self.raw_table_df = raw_table_df

    # ── PySpark-only helpers ──────────────────────────────────────────────────

    @staticmethod
    def _build_situation_struct(
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
    ):
        """PySpark Column equivalent of :meth:`_format_case_object`.

        Column references (e.g. ``'overlap_dp_record_id'``) are resolved at
        evaluation time against the DataFrame the struct is applied to.
        SQL expressions (e.g. ``"TIMESTAMP '9999-12-31 23:59:59'"`` or
        ``"src_dp_ts_from - INTERVAL '1' SECOND"``) are handled via
        ``F.expr()``.
        """

        def ts_or_null(val):
            return F.lit(None).cast("timestamp") if val is None else F.expr(str(val))

        def key_or_null(val):
            return F.lit(None).cast("string") if val is None else F.col(str(val))

        def bool_or_null(val):
            if val is None:
                return F.lit(None).cast("boolean")
            s = str(val).strip().lower()
            if s == "true":
                return F.lit(True)
            if s == "false":
                return F.lit(False)
            return F.col(str(val))  # column reference, e.g. 'next_dp_is_active'

        return F.struct(
            F.lit(name).alias("name"),
            F.lit(is_upd).alias("is_upd"),
            key_or_null(upd_key).alias("upd_key"),
            ts_or_null(upd_dp_ts_from).alias("upd_dp_ts_from"),
            ts_or_null(upd_dp_ts_to).alias("upd_dp_ts_to"),
            bool_or_null(upd_dp_is_active).alias("upd_dp_is_active"),
            bool_or_null(upd_dp_is_latest).alias("upd_dp_is_latest"),
            F.lit(is_upd_2).alias("is_upd_2"),
            key_or_null(upd_key_2).alias("upd_key_2"),
            ts_or_null(upd_dp_ts_from_2).alias("upd_dp_ts_from_2"),
            ts_or_null(upd_dp_ts_to_2).alias("upd_dp_ts_to_2"),
            bool_or_null(upd_dp_is_active_2).alias("upd_dp_is_active_2"),
            bool_or_null(upd_dp_is_latest_2).alias("upd_dp_is_latest_2"),
            F.lit(is_ins).alias("is_ins"),
            ts_or_null(ins_dp_ts_from).alias("ins_dp_ts_from"),
            ts_or_null(ins_dp_ts_to).alias("ins_dp_ts_to"),
            bool_or_null(ins_dp_is_active).alias("ins_dp_is_active"),
            bool_or_null(ins_dp_is_latest).alias("ins_dp_is_latest"),
            F.lit(is_del).alias("is_del"),
            key_or_null(del_key).alias("del_key"),
            F.lit(is_del_2).alias("is_del_2"),
            key_or_null(del_key_2).alias("del_key_2"),
        )

    def _build_staging_df(
        self,
        dp_ts: datetime,
    ) -> DataFrame:
        """Build the staging DataFrame (PySpark equivalent of the SQL CTE chain
        ``changed_records → records_to_process → prepared_source``).

        Returns a DataFrame with columns::

            merge_record_id, dp_record_id, <pk_cols>, <val_cols>, dp_record_hash,
            dp_ts, dp_del_flag, operation_type, case_name,
            dp_ts_from, dp_ts_to, dp_is_active, dp_is_latest
        """
        dp_ts_str = dp_ts.strftime("%Y-%m-%d %H:%M:%S")
        all_cols = self.cols_bks + self.cols_val

        # ── src_records ───────────────────────────────────────────────────────
        hash_expr = F.upper(
            F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in all_cols]), 256)
        )
        src_df = (
            #self.spark.table(self.raw_table_fqn())
            self.raw_table_df
            .filter(F.col(self.col_dp_ts_filter) == F.expr(f"TIMESTAMP '{dp_ts_str}'"))
            .select(
                *[F.col(c) for c in self.cols_bks],
                *[F.col(c) for c in self.cols_val],
                F.col(self.col_dp_ts).alias("src_dp_ts_from"),
                F.col(self.col_dp_ts_filter).alias("dp_ts"),
                F.expr(f"CASE WHEN {self.delta_mode_delete_expression} THEN 'INACTIVE' ELSE 'ACTIVE' END")
                    .alias("dp_del_flag") if self.delta_mode_delete_expression else F.col("status").alias("dp_del_flag"),
                hash_expr.alias("src_dp_record_hash"),
            )
        )

        # ── sub-DataFrames from scd2 table (all conflicting columns pre-renamed) ─
        scd2_df = self.spark.table(self.scd2_table_fqn())

        overlap_df = scd2_df.select(
            *[F.col(c).alias(f"overlap_{c}") for c in self.cols_bks],
            F.col("dp_record_hash").alias("overlap_dp_record_hash"),
            F.col("dp_record_id").alias("overlap_dp_record_id"),
            F.col("dp_ts_from").alias("overlap_dp_ts_from"),
            F.col("dp_ts_to").alias("overlap_dp_ts_to"),
            F.col("dp_is_active").alias("overlap_dp_is_active"),
            F.col("dp_is_latest").alias("overlap_dp_is_latest"),
        )

        prev_df = scd2_df.select(
            *[F.col(c).alias(f"prev_{c}") for c in self.cols_bks],
            F.col("dp_record_id").alias("prev_dp_record_id"),
            F.col("dp_record_hash").alias("prev_dp_record_hash"),
            F.col("dp_ts_from").alias("prev_dp_ts_from"),
            F.col("dp_ts_to").alias("prev_dp_ts_to"),
            F.col("dp_is_active").alias("prev_dp_is_active"),
            F.col("dp_is_latest").alias("prev_dp_is_latest"),
        )

        next_df = scd2_df.filter(F.col("dp_is_active") == True).select(
            *[F.col(c).alias(f"next_{c}") for c in self.cols_bks],
            F.col("dp_record_id").alias("next_dp_record_id"),
            F.col("dp_record_hash").alias("next_dp_record_hash"),
            F.col("dp_ts_from").alias("next_dp_ts_from"),
            F.col("dp_ts_to").alias("next_dp_ts_to"),
            F.col("dp_is_active").alias("next_dp_is_active"),
            F.col("dp_is_latest").alias("next_dp_is_latest"),
        )

        # ── join conditions ───────────────────────────────────────────────────
        def null_safe_pk_cond(prefix):
            return reduce(
                lambda a, b: a & b,
                [F.col(c).eqNullSafe(F.col(f"{prefix}_{c}")) for c in self.cols_bks],
            )

        overlap_cond = null_safe_pk_cond("overlap") & F.col("src_dp_ts_from").between(
            F.col("overlap_dp_ts_from"), F.col("overlap_dp_ts_to")
        )

        prev_cond = null_safe_pk_cond("prev") & (
            (F.col("prev_dp_ts_to") == (F.col("src_dp_ts_from") - F.expr(_ONE_SEC)))
            | (
                (F.col("prev_dp_ts_to") < F.col("src_dp_ts_from"))
                & (F.col("prev_dp_is_latest") == True)
            )
        )

        next_cond = null_safe_pk_cond("next") & (
            F.col("src_dp_ts_from") < F.col("next_dp_ts_from")
        )

        # ── changed_records (3-way LEFT JOIN) ─────────────────────────────────
        changed_df = (
            src_df.join(overlap_df, overlap_cond, "left")
            .join(prev_df, prev_cond, "left")
            .join(next_df, next_cond, "left")
        )

        # ── comparison flags ──────────────────────────────────────────────────
        def same_as_src(hash_col):
            return (
                F.when(F.col(hash_col).isNull(), F.lit(None).cast("boolean"))
                .when(F.col("src_dp_record_hash") == F.col(hash_col), F.lit(True))
                .otherwise(F.lit(False))
            )

        records_df = (
            changed_df.withColumn(
                "overlap_is_same_as_src", same_as_src("overlap_dp_record_hash")
            )
            .withColumn("prev_is_same_as_src", same_as_src("prev_dp_record_hash"))
            .withColumn("next_is_same_as_src", same_as_src("next_dp_record_hash"))
            .withColumn(
                "prev_with_gap",
                F.col("prev_dp_ts_to") < (F.col("src_dp_ts_from") - F.expr(_ONE_SEC)),
            )
        )

        # ── situation struct (CASE WHEN) ──────────────────────────────────────
        s = self._build_situation_struct
        c = F.col

        situation_col = (
            F.when(
                c("prev_is_same_as_src").isNull()
                & c("overlap_is_same_as_src").isNull()
                & c("next_is_same_as_src").isNull()
                & c("overlap_dp_is_active").isNull()
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_1",
                    is_ins=True,
                    ins_dp_ts_from="src_dp_ts_from",
                    ins_dp_ts_to=_MAX_TS,
                ),
            )
            .when(
                c("prev_is_same_as_src").isNull()
                & (c("overlap_is_same_as_src") == True)
                & c("next_is_same_as_src").isNull()
                & (c("overlap_dp_is_active") == True)
                & (c("dp_del_flag") == "ACTIVE"),
                s("CASE_9"),
            )
            .when(
                c("prev_is_same_as_src").isNull()
                & (c("overlap_is_same_as_src") == True)
                & c("next_is_same_as_src").isNull()
                & (c("overlap_dp_is_active") == False)
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_10",
                    is_upd=True,
                    upd_key="overlap_dp_record_id",
                    upd_dp_ts_from="overlap_dp_ts_from",
                    upd_dp_ts_to=_MAX_TS,
                    upd_dp_is_active="True",
                    upd_dp_is_latest="True",
                ),
            )
            .when(
                c("prev_is_same_as_src").isNull()
                & (c("overlap_is_same_as_src") == False)
                & c("next_is_same_as_src").isNull()
                & (
                    (c("overlap_dp_is_active") == True)
                    | (c("overlap_dp_is_active") == False)
                )
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_11",
                    is_upd=True,
                    upd_key="overlap_dp_record_id",
                    upd_dp_ts_from="overlap_dp_ts_from",
                    upd_dp_ts_to=f"src_dp_ts_from - {_ONE_SEC}",
                    upd_dp_is_active="False",
                    upd_dp_is_latest="False",
                    is_ins=True,
                    ins_dp_ts_from="src_dp_ts_from",
                    ins_dp_ts_to=_MAX_TS,
                ),
            )
            .when(
                c("prev_is_same_as_src").isNull()
                & (c("overlap_is_same_as_src") == True)
                & (c("next_is_same_as_src") == False)
                & (c("overlap_dp_is_active") == False)
                & (c("dp_del_flag") == "ACTIVE"),
                s("CASE_12"),
            )
            .when(
                c("prev_is_same_as_src").isNull()
                & (c("overlap_is_same_as_src") == False)
                & (c("next_is_same_as_src") == False)
                & (c("overlap_dp_is_active") == False)
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_13",
                    is_upd=True,
                    upd_key="overlap_dp_record_id",
                    upd_dp_ts_from="overlap_dp_ts_from",
                    upd_dp_ts_to=f"src_dp_ts_from - {_ONE_SEC}",
                    upd_dp_is_active="False",
                    upd_dp_is_latest="False",
                    is_ins=True,
                    ins_dp_ts_from="src_dp_ts_from",
                    ins_dp_ts_to="overlap_dp_ts_to",
                    ins_dp_is_active="False",
                    ins_dp_is_latest="False",
                ),
            )
            .when(
                c("prev_is_same_as_src").isNull()
                & (c("overlap_is_same_as_src") == False)
                & (c("next_is_same_as_src") == True)
                & (c("overlap_dp_is_active") == False)
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_14",
                    is_upd=True,
                    upd_key="overlap_dp_record_id",
                    upd_dp_ts_from="overlap_dp_ts_from",
                    upd_dp_ts_to=f"src_dp_ts_from - {_ONE_SEC}",
                    upd_dp_is_active="False",
                    upd_dp_is_latest="False",
                    is_upd_2=True,
                    upd_key_2="next_dp_record_id",
                    upd_dp_ts_from_2="src_dp_ts_from",
                    upd_dp_ts_to_2="next_dp_ts_to",
                ),
            )
            .when(
                c("prev_is_same_as_src").isNull()
                & (c("overlap_is_same_as_src") == True)
                & (c("next_is_same_as_src") == False)
                & (c("overlap_dp_is_active") == False)
                & (c("dp_del_flag") == "ACTIVE"),
                s("CASE_15"),
            )
            .when(
                (c("prev_is_same_as_src") == False)
                & (c("overlap_is_same_as_src") == False)
                & (c("next_is_same_as_src") == False)
                & (c("overlap_dp_is_active") == False)
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_16",
                    is_upd=True,
                    upd_key="overlap_dp_record_id",
                    upd_dp_ts_from="overlap_dp_ts_from",
                    upd_dp_ts_to="overlap_dp_ts_to",
                    upd_dp_is_active="False",
                    upd_dp_is_latest="False",
                ),
            )
            .when(
                (c("prev_is_same_as_src") == True)
                & (c("overlap_is_same_as_src") == False)
                & (c("next_is_same_as_src") == True)
                & (c("overlap_dp_is_active") == False)
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_17",
                    is_upd=True,
                    upd_key="next_dp_record_id",
                    upd_dp_ts_from="prev_dp_ts_from",
                    upd_dp_ts_to="next_dp_ts_to",
                    is_del=True,
                    del_key="overlap_dp_record_id",
                    is_del_2=True,
                    del_key_2="prev_dp_record_id",
                ),
            )
            .when(
                c("prev_is_same_as_src").isNull()
                & c("overlap_is_same_as_src").isNull()
                & (c("next_is_same_as_src") == True)
                & c("overlap_dp_is_active").isNull()
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_18",
                    is_upd=True,
                    upd_key="next_dp_record_id",
                    upd_dp_ts_from="src_dp_ts_from",
                    upd_dp_ts_to="next_dp_ts_to",
                ),
            )
            .when(
                c("prev_is_same_as_src").isNull()
                & c("overlap_is_same_as_src").isNull()
                & (c("next_is_same_as_src") == False)
                & c("overlap_dp_is_active").isNull()
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_19",
                    is_ins=True,
                    ins_dp_ts_from="src_dp_ts_from",
                    ins_dp_ts_to=f"next_dp_ts_from - {_ONE_SEC}",
                    ins_dp_is_active="False",
                    ins_dp_is_latest="False",
                ),
            )
            .when(
                (c("prev_is_same_as_src") == True)
                & c("overlap_is_same_as_src").isNull()
                & c("next_is_same_as_src").isNull()
                & c("overlap_dp_is_active").isNull()
                & (c("prev_with_gap") == False)
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_20",
                    is_upd=True,
                    upd_key="prev_dp_record_id",
                    upd_dp_ts_from="prev_dp_ts_from",
                    upd_dp_ts_to=_MAX_TS,
                    upd_dp_is_active="True",
                    upd_dp_is_latest="True",
                ),
            )
            .when(
                (c("prev_is_same_as_src") == False)
                & c("overlap_is_same_as_src").isNull()
                & c("next_is_same_as_src").isNull()
                & c("overlap_dp_is_active").isNull()
                & (c("prev_with_gap") == False)
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_21",
                    is_upd=True,
                    upd_key="prev_dp_record_id",
                    upd_dp_ts_from="prev_dp_ts_from",
                    upd_dp_ts_to="prev_dp_ts_to",
                    upd_dp_is_active="False",
                    upd_dp_is_latest="False",
                    is_ins=True,
                    ins_dp_ts_from="src_dp_ts_from",
                    ins_dp_ts_to=_MAX_TS,
                    ins_dp_is_active="True",
                    ins_dp_is_latest="True",
                ),
            )
            .when(
                (c("prev_is_same_as_src") == True)
                & c("overlap_is_same_as_src").isNull()
                & c("next_is_same_as_src").isNull()
                & c("overlap_dp_is_active").isNull()
                & (c("prev_with_gap") == True)
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_22",
                    is_upd=True,
                    upd_key="prev_dp_record_id",
                    upd_dp_ts_from="prev_dp_ts_from",
                    upd_dp_ts_to="prev_dp_ts_to",
                    upd_dp_is_active="False",
                    upd_dp_is_latest="False",
                    is_ins=True,
                    ins_dp_ts_from="src_dp_ts_from",
                    ins_dp_ts_to=_MAX_TS,
                    ins_dp_is_active="True",
                    ins_dp_is_latest="True",
                ),
            )
            .when(
                (c("prev_is_same_as_src") == False)
                & c("overlap_is_same_as_src").isNull()
                & c("next_is_same_as_src").isNull()
                & c("overlap_dp_is_active").isNull()
                & (c("prev_with_gap") == True)
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_23",
                    is_upd=True,
                    upd_key="prev_dp_record_id",
                    upd_dp_ts_from="prev_dp_ts_from",
                    upd_dp_ts_to="prev_dp_ts_to",
                    upd_dp_is_active="False",
                    upd_dp_is_latest="False",
                    is_ins=True,
                    ins_dp_ts_from="src_dp_ts_from",
                    ins_dp_ts_to=_MAX_TS,
                    ins_dp_is_active="True",
                    ins_dp_is_latest="True",
                ),
            )
            .when(
                (c("prev_is_same_as_src") == False)
                & c("overlap_is_same_as_src").isNull()
                & (c("next_is_same_as_src") == True)
                & c("overlap_dp_is_active").isNull()
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_24",
                    is_upd=True,
                    upd_key="next_dp_record_id",
                    upd_dp_ts_from="src_dp_ts_from",
                    upd_dp_ts_to="next_dp_ts_to",
                ),
            )
            .when(
                (c("prev_is_same_as_src") == True)
                & c("overlap_is_same_as_src").isNull()
                & (c("next_is_same_as_src") == False)
                & c("overlap_dp_is_active").isNull()
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_25",
                    is_upd=True,
                    upd_key="prev_dp_record_id",
                    upd_dp_ts_from="prev_dp_ts_from",
                    upd_dp_ts_to=f"next_dp_ts_from - {_ONE_SEC}",
                ),
            )
            .when(
                (c("prev_is_same_as_src") == True)
                & c("overlap_is_same_as_src").isNull()
                & (c("next_is_same_as_src") == True)
                & c("overlap_dp_is_active").isNull()
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_26",
                    is_upd=True,
                    upd_key="prev_dp_record_id",
                    upd_dp_ts_from="prev_dp_ts_from",
                    upd_dp_ts_to="next_dp_ts_to",
                    upd_dp_is_active="next_dp_is_active",
                    upd_dp_is_latest="next_dp_is_latest",
                    is_del=True,
                    del_key="next_dp_record_id",
                ),
            )
            .when(
                (c("prev_is_same_as_src") == False)
                & c("overlap_is_same_as_src").isNull()
                & (c("next_is_same_as_src") == False)
                & c("overlap_dp_is_active").isNull()
                & (c("dp_del_flag") == "ACTIVE"),
                s(
                    "CASE_27",
                    is_ins=True,
                    ins_dp_ts_from="src_dp_ts_from",
                    ins_dp_ts_to=f"next_dp_ts_from - {_ONE_SEC}",
                    ins_dp_is_active="False",
                    ins_dp_is_latest="False",
                ),
            )
            .when(
                c("prev_is_same_as_src").isNull()
                & (c("overlap_is_same_as_src") == True)
                & c("next_is_same_as_src").isNull()
                & (c("overlap_dp_is_active") == True)
                & (c("dp_del_flag") == "INACTIVE"),
                s(
                    "CASE_30",
                    is_upd=True,
                    upd_key="overlap_dp_record_id",
                    upd_dp_ts_from="overlap_dp_ts_from",
                    upd_dp_ts_to=f"src_dp_ts_from - {_ONE_SEC}",
                    upd_dp_is_active="False",
                    upd_dp_is_latest="True",
                ),
            )
            .when(
                c("prev_is_same_as_src").isNull()
                & (c("overlap_is_same_as_src") == True)
                & c("next_is_same_as_src").isNull()
                & (c("overlap_dp_is_active") == False)
                & (c("dp_del_flag") == "INACTIVE"),
                s(
                    "CASE_31",
                    is_upd=True,
                    upd_key="overlap_dp_record_id",
                    upd_dp_ts_from="overlap_dp_ts_from",
                    upd_dp_ts_to=f"src_dp_ts_from - {_ONE_SEC}",
                    upd_dp_is_active="False",
                    upd_dp_is_latest="True",
                ),
            )
            .when(
                c("prev_is_same_as_src").isNull()
                & (c("overlap_is_same_as_src") == False)
                & c("next_is_same_as_src").isNull()
                & (c("overlap_dp_is_active") == True)
                & (c("dp_del_flag") == "INACTIVE"),
                s(
                    "CASE_32",
                    is_upd=True,
                    upd_key="overlap_dp_record_id",
                    upd_dp_ts_from="overlap_dp_ts_from",
                    upd_dp_ts_to=f"src_dp_ts_from - {_ONE_SEC}",
                    upd_dp_is_active="False",
                    upd_dp_is_latest="True",
                ),
            )
            .when(
                (c("prev_is_same_as_src") == True)
                & (c("overlap_is_same_as_src") == False)
                & (c("next_is_same_as_src") == False)
                & (c("overlap_dp_is_active") == True)
                & (c("dp_del_flag") == "INACTIVE"),
                s(
                    "CASE_33",
                    is_upd=True,
                    upd_key="prev_dp_record_id",
                    upd_dp_ts_from="prev_dp_ts_from",
                    upd_dp_ts_to="prev_dp_ts_to",
                    upd_dp_is_active="False",
                    upd_dp_is_latest="True",
                    is_del=True,
                    del_key="overlap_dp_record_id",
                ),
            )
            .otherwise(F.lit(None))
        )

        records_df = records_df.withColumn("situation", situation_col)

        # ── prepared_source (UNION ALL of the 5 operation branches) ──────────
        pk_cols = [F.col(c) for c in self.cols_bks]
        val_cols = [F.col(c) for c in self.cols_val]

        null_ts = F.lit(None).cast("timestamp")
        null_bool = F.lit(None).cast("boolean")

        def _common(op_type):
            return [
                *pk_cols,
                *val_cols,
                F.col("src_dp_record_hash").alias("dp_record_hash"),
                F.col("dp_ts"),
                F.col("dp_del_flag"),
                F.lit(op_type).alias("operation_type"),
                F.col("situation.name").alias("case_name"),
            ]

        updates_df = records_df.filter(F.col("situation.is_upd") == True).select(
            F.col("situation.upd_key").alias("merge_record_id"),
            F.col("situation.upd_key").alias("dp_record_id"),
            *_common("UPDATE_VERSION"),
            F.col("situation.upd_dp_ts_from").alias("dp_ts_from"),
            F.col("situation.upd_dp_ts_to").alias("dp_ts_to"),
            F.col("situation.upd_dp_is_active").alias("dp_is_active"),
            F.col("situation.upd_dp_is_latest").alias("dp_is_latest"),
        )

        updates_2_df = records_df.filter(F.col("situation.is_upd_2") == True).select(
            F.col("situation.upd_key_2").alias("merge_record_id"),
            F.col("situation.upd_key_2").alias("dp_record_id"),
            *_common("UPDATE_VERSION"),
            F.col("situation.upd_dp_ts_from_2").alias("dp_ts_from"),
            F.col("situation.upd_dp_ts_to_2").alias("dp_ts_to"),
            F.col("situation.upd_dp_is_active_2").alias("dp_is_active"),
            F.col("situation.upd_dp_is_latest_2").alias("dp_is_latest"),
        )

        inserts_df = records_df.filter(F.col("situation.is_ins") == True).select(
            F.lit(None).cast("string").alias("merge_record_id"),
            F.expr("uuid()").alias("dp_record_id"),
            *_common("INSERT_NEW_VERSION"),
            F.col("situation.ins_dp_ts_from").alias("dp_ts_from"),
            F.col("situation.ins_dp_ts_to").alias("dp_ts_to"),
            F.col("situation.ins_dp_is_active").alias("dp_is_active"),
            F.col("situation.ins_dp_is_latest").alias("dp_is_latest"),
        )

        deletes_df = records_df.filter(F.col("situation.is_del") == True).select(
            F.col("situation.del_key").alias("merge_record_id"),
            F.col("situation.del_key").alias("dp_record_id"),
            *_common("DELETE_VERSION"),
            null_ts.alias("dp_ts_from"),
            null_ts.alias("dp_ts_to"),
            null_bool.alias("dp_is_active"),
            null_bool.alias("dp_is_latest"),
        )

        deletes_2_df = records_df.filter(F.col("situation.is_del_2") == True).select(
            F.col("situation.del_key_2").alias("merge_record_id"),
            F.col("situation.del_key_2").alias("dp_record_id"),
            *_common("DELETE_VERSION"),
            null_ts.alias("dp_ts_from"),
            null_ts.alias("dp_ts_to"),
            null_bool.alias("dp_is_active"),
            null_bool.alias("dp_is_latest"),
        )

        return (
            updates_df.unionByName(updates_2_df)
            .unionByName(inserts_df)
            .unionByName(deletes_df)
            .unionByName(deletes_2_df)
        )

    # ── Overridden operations ─────────────────────────────────────────────────

    def materialize_view(self, scd2_intermediary_table_name: str) -> str:
        """Override: use PySpark DataFrame write instead of SQL CREATE TABLE AS SELECT."""
        mv_table = f"{scd2_intermediary_table_name}_mv"
        mv_fqn = self._fqn(mv_table)

        self.spark.sql(f"DROP TABLE IF EXISTS {mv_fqn}")

        s3_path = f"warehouse/{mv_table}"
        #self.delete_s3_location(
        #    s3_client=self.s3_client, bucket="admin-bucket", path=s3_path
        #)

        (
            self.spark.table(scd2_intermediary_table_name)
            .write.format("iceberg")
            .saveAsTable(mv_fqn)
        )

        logger.info(f"Materialized {mv_fqn} via PySpark write.")
        return mv_table

    def merge_into_scd2_table(
        self,
        dp_ts: datetime,
        current_ts: Optional[datetime] = None,
        show_input_to_merge: bool = False,
        output_file_name: Optional[str] = None,
    ):
        """Override: build staging DataFrame with PySpark, then run MERGE as Spark SQL."""
        staging_df = self._build_staging_df(
            #source_df=self.source_df,
            dp_ts=dp_ts,
        )

        #staging_df.show(truncate=False)
        # We register the staging DataFrame as a temp view to be used as the source in the MERGE statement.  We can't use the DataFrame directly in the MERGE.
        staging_df.createOrReplaceTempView(self.scd2_intermediary_table_name)
        logger.info(
            f"SCD2 staging view '{self.scd2_intermediary_table_name}' registered."
        )

        # have to materialize the view because of the UUID() function used for generating dp_record_id for inserts - Spark doesn't allow non-deterministic functions in MERGE source!
        if self.materialize_data_before_merge:
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
                raise e

    def merge_into_scd2_table_and_return_as_df(
        self,
        dp_ts: datetime,
        current_ts: Optional[datetime] = None,
        show_input_to_merge: bool = False,
        output_file_name: Optional[str] = None,
    ) -> DataFrame:
        """Override: build staging DataFrame with PySpark, then run MERGE as Spark SQL, returning the post-merge SCD2 table as a DataFrame."""
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
        """Override: use PySpark DataFrame API instead of SQL SELECT.

        For INTERMEDIARY the temp view is accessed by short name (no database
        prefix) since it is registered via ``createOrReplaceTempView``.
        """
        if table == SCD2Table.INTERMEDIARY:
            # Temp view — registered without a database prefix
            df = self.spark.table(self.scd2_intermediary_table_name)
        else:
            table_fqn = self._resolve_table_fqn(table, iceberg_meta_tablename=iceberg_meta_tablename)
            if for_version is not None:
                df = (
                    self.spark.read.format("iceberg")
                    .option("snapshot-id", str(for_version))
                    .load(table_fqn)
                )
            else:
                df = self.spark.table(table_fqn)

        if exclude_cols:
            df = df.drop(*exclude_cols)

        if order_by_cols:
            df = df.orderBy(*[F.col(c).asc_nulls_last() for c in order_by_cols])

        return df.toPandas()
