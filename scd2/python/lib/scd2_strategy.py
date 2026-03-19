from abc import ABC, abstractmethod
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SCD2Strategy(ABC):
    """Abstract base class defining the SCD2 merge strategy interface.

    Concrete implementations provide engine-specific SQL generation and
    execution (e.g. Trino, Spark).  Each instance owns a single execution
    context (connection, SparkSession, …) and a namespace (catalog/schema or
    database) so callers do not have to repeat those arguments on every call.
    """

    def __init__(self, raw_table_name: str, scd2_table_name: str, scd2_intermediary_table_name: str = None):
        self.raw_table_name = raw_table_name
        self.scd2_table_name = scd2_table_name
        self.scd2_intermediary_table_name = scd2_intermediary_table_name or f"{scd2_table_name}_temp"

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

    @staticmethod
    def delete_s3_location(s3_client, bucket: str, path: str) -> None:
        """Delete all objects under the specified S3 location."""
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=path)
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
            logger.info(f"S3 folder s3a://{bucket}/{path} deleted successfully.")
        except Exception as e:
            logger.warning(f"Failed to delete S3 folder: {e}")

    def raw_table_fqn(
        self, iceberg_meta_tablename: str = None
    ) -> str:
        """Return the fully qualified name for the raw table."""
        if iceberg_meta_tablename:
            return self._fqn(f"{self.raw_table_name}${iceberg_meta_tablename}")
        else:
            return self._fqn(self.raw_table_name)

    def scd2_table_fqn(
        self, iceberg_meta_tablename: str = None
    ) -> str:
        """Return the fully qualified name for the SCD2 table."""
        if iceberg_meta_tablename:
            return self._fqn(f"{self.scd2_table_name}${iceberg_meta_tablename}")
        else:
            return self._fqn(self.scd2_table_name)

    def scd2_intermediary_table_fqn(
        self, iceberg_meta_tablename: str = None
    ) -> str:
        """Return the fully qualified name for the SCD2 intermediate table."""
        if iceberg_meta_tablename:
            return self._fqn(f"{self.scd2_intermediary_table_name}${iceberg_meta_tablename}")
        else:
            return self._fqn(self.scd2_intermediary_table_name)

    # ── Abstract SQL formatters ─────────────────────────────────────────────

    @abstractmethod
    def _fqn(
        self,
        object_name: str
    ) -> str:
        """Return the fully qualified name for the given object."""

    @abstractmethod
    def format_view(
        self,
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
        pk_columns: list,
        val_columns: list,
    ) -> str:
        """Return the MERGE INTO statement that applies SCD2 changes to the dimension table."""

    # ── Abstract operations ─────────────────────────────────────────────────

    @abstractmethod
    def create_dim_table(
        self,
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

    @abstractmethod
    def get_table_data(
        self,
        table_name: str,
        exclude_cols: list = [],
        order_by_cols: list = [],
        for_version: str = None,
    ) -> "pd.DataFrame":
        """Read table data and return as a pandas DataFrame.

        Args:
            table_name: Short (unqualified) table name; the strategy applies its
                own namespace prefix via ``_fqn()``.
            exclude_cols: Column names to omit from the result.
            order_by_cols: Columns to sort by.
            for_version: Optional snapshot identifier (Trino: ``FOR VERSION AS OF``,
                Spark: ``VERSION AS OF``).
        """

    @abstractmethod
    def optimize_table(self, table_name: str) -> None:
        """Optimize the specified table.

        Returns:
           None
        """

    @abstractmethod
    def analyze_table(self, table_name: str) -> None:
        """Analyze the specified table.

        Returns:
           None
        """
