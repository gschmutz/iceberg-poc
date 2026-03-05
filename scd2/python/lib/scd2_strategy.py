from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional


class SCD2Strategy(ABC):
    """Abstract base class defining the SCD2 merge strategy interface.

    Concrete implementations provide engine-specific SQL generation and
    execution (e.g. Trino, Spark).  Each instance owns a single execution
    context (connection, SparkSession, …) and a namespace (catalog/schema or
    database) so callers do not have to repeat those arguments on every call.
    """

    # ── Shared utility methods ──────────────────────────────────────────────

    @staticmethod
    def add_prefix(values: list, prefix: str) -> list:
        """Prepend a qualifier prefix to each column name."""
        if prefix:
            return [f"{prefix}.{v}" for v in values]
        return values

    @staticmethod
    def format_values(values: list) -> str:
        """Join a list of column expressions into a comma-separated string."""
        return ", ".join(str(v) for v in values)

    # ── Abstract SQL formatters ─────────────────────────────────────────────

    @abstractmethod
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
        """Return the SQL that creates (or replaces) the SCD2 staging view."""

    @abstractmethod
    def format_merge(
        self,
        current_ts: datetime,
        dim_table_name: str,
        scd2_view_name: str,
        pk_columns: list,
        val_columns: list,
    ) -> str:
        """Return the MERGE INTO statement that applies SCD2 changes to the dimension table."""

    # ── Abstract operations ─────────────────────────────────────────────────

    @abstractmethod
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
        """Drop and (re)create the SCD2 dimension table."""

    @abstractmethod
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
        """Execute the full SCD2 merge pipeline.

        Returns:
            (result, iceberg_metadata) – both may be None when perform_merge_op=False.
        """
