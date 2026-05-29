import logging
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Optional
from pyspark.sql import DataFrame

if TYPE_CHECKING:
    import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SCD2Table(Enum):
    """Identifies one of the three logical Apache Iceberg tables in the SCD2 pipeline.

    Attributes:
        RAW: The staging table that receives new source records for each load batch.
            Partitioned by the load-timestamp column so that each batch can be
            isolated and re-processed independently.
        SCD2: The SCD2 dimension table that holds the full version history.
            Every row represents a single version of an entity, bounded by
            ``dp_ts_from`` / ``dp_ts_to`` and annotated with ``dp_is_active``
            and ``dp_is_latest``.
        INTERMEDIARY: A temporary view or materialized table produced between the
            raw read and the final ``MERGE INTO``.  It contains the classified
            change records (NEW / CHANGED / DELETED / UNCHANGED) together with
            the ``merge_key`` column that drives the MERGE logic.
    """

    RAW = "raw"
    SCD2 = "scd2"
    INTERMEDIARY = "intermediary"


class SCD2Strategy(ABC):
    """Abstract base class for engine-specific SCD2 merge implementations.

    SCD2Strategy defines the full contract for loading a batch of source records
    into a Slowly Changing Dimension Type 2 (SCD2) Apache Iceberg table.  Each
    concrete subclass targets a specific SQL engine:

    * ``TrinoSCD2Strategy``  – Trino / Iceberg via Hive Metastore
    * ``SparkSCD2Strategy``  – Apache Spark SQL / Iceberg
    * ``PySparkSCD2Strategy`` – PySpark DataFrame API + Spark SQL MERGE

    Merge pipeline (view-then-merge pattern)
    -----------------------------------------
    1. Source records for the current batch are read from the **raw table**
       filtered on ``col_dp_ts_filter``.
    2. A SHA-256 row hash is computed over all business-key and value columns
       (``concat_ws('||', ...)`` + SHA-256) for change detection.
    3. Each source record is FULL OUTER JOINed against the existing SCD2 table
       to classify it as NEW / CHANGED / DELETED / UNCHANGED.
    4. Changed records emit **two** rows in the staging view:
       - one with ``merge_key = <pk>`` → matches an existing row → closes the
         old version (UPDATE sets ``dp_ts_to``, ``dp_is_active``, ``dp_is_latest``).
       - one with ``merge_key = NULL`` → no match → inserts the new version.
    5. ``MERGE INTO <scd2_table> USING <intermediary_view>`` applies all changes
       atomically via a single engine MERGE statement.

    SCD2 dimension metadata columns
    --------------------------------
    Every row in the SCD2 table carries the following engine-managed columns:

    * ``dp_record_id``   – UUID surrogate key (unique per version row).
    * ``dp_ts_from``     – Effective start timestamp of this version.
    * ``dp_ts_to``       – Effective end timestamp (``9999-12-31 23:59:59`` = open).
    * ``dp_is_active``   – ``True`` while the entity is not soft-deleted.
    * ``dp_is_latest``   – ``True`` for the most recent version (even if deleted).
    * ``dp_created_at``  – Timestamp when this version row was first inserted.
    * ``dp_replaced_at`` – Timestamp when this version was superseded (or NULL).
    * ``dp_record_hash`` – SHA-256 hash of all business columns for change detection.

    Typical usage
    --------------
    ::

        strategy = TrinoSCD2Strategy(
            conn, s3_client,
            catalog="iceberg_hive", schema="default",
            source_table_name="raw_person",
            scd2_table_name="dim_person",
            cols_bks=["id"],
            cols_val=["first_name", "last_name", "city"],
            col_dp_ts_filter="dp_ts",
            use_logical_delete_for_source_table=False,
            perform_merge_op=True,
        )
        result, metadata = strategy.merge_into_scd2_table(
            dp_ts=datetime(2024, 1, 15, 10, 0, 0),
            current_ts=datetime.now(),
        )
    """

    def __init__(
        self,
        scd2_intermediary_table_name: str = None,
        cols_bks: Optional[list] = None,
        cols_val: Optional[list] = None,
        use_logical_delete_for_source_table: bool = False,
        logical_delete_expression: Optional[str] = None,
        materialize_data_before_merge: bool = False,
        check_physical_delete_against_source_table: bool = False,
        perform_merge_op: bool = True,
        col_dp_valid_from: str = "dp_ts_from",
        col_dp_valid_to: str = "dp_ts_to",
        col_dp_created_at: str = "dp_created_at",
        col_dp_replaced_at: str = "dp_replaced_at",
        col_dp_ts: str = "dp_ts_version",
        col_dp_ts_filter: str = "dp_ts",
    ):
        """Initialise the strategy with table names, column definitions, and behavior flags.

        Args:
            scd2_intermediary_table_name: Unqualified name for the intermediary
                view or materialized staging table used between change detection
                and the MERGE statement.  Defaults to ``<scd2_table_name>_temp``.
            cols_bks: Business-key column names used to identify an entity across
                versions (e.g. ``["id"]`` or ``["tenant_id", "user_id"]``).
            cols_val: Value column names whose changes trigger a new SCD2 version
                (e.g. ``["first_name", "last_name", "city"]``).
            use_logical_delete_for_source_table: Controls how the absence of an entity
                in the current raw batch is interpreted.

                * ``False`` (default, full-snapshot mode) – an entity that is
                  present in the SCD2 table but absent from the raw batch is
                  treated as deleted and a soft-delete version is written.
                * ``True`` (delta / CDC mode) – absent entities are ignored;
                  only records explicitly present in the raw batch are processed.
            logical_delete_expression: When ``use_logical_delete_for_source_table`` is ``True`` this expression defines how to identify deleted records in the raw table (e.g. ``"status = 'INACTIVE'"``).  Required when delta mode is enabled.
            check_physical_delete_against_source_table: check physical deletes against source table or scd2 table?
            materialize_data_before_merge: When ``True`` the intermediary staging
                view is written to a physical Iceberg table before the MERGE is
                executed.  Required for engines (e.g. Spark) that do not allow
                non-deterministic functions such as ``uuid()`` inside a MERGE
                source query.  Defaults to ``False``.
            perform_merge_op: When ``False`` the pipeline stops after creating the
                intermediary view, allowing callers to inspect the staged records
                without modifying the SCD2 table.  Defaults to ``True``.
            col_dp_valid_from: Name of the timestamp column in the SCD2 table that
                identifies the version start (e.g. ``"dp_ts_from"``).  Defaults to ``"dp_ts_from"``.
            col_dp_valid_to: Name of the timestamp column in the SCD2 table that identifies the version end (e.g. ``"dp_ts_to"``).  Defaults to ``"dp_ts_to"``.
            col_dp_created_at: Name of the timestamp column in the SCD2 table that identifies when a version was created (e.g. ``"dp_created_at"``).  Defaults to ``"dp_created_at"``.
            col_dp_replaced_at: Name of the timestamp column in the SCD2 table that identifies when a version was superseded (e.g. ``"dp_replaced_at"``).  Defaults to ``"dp_replaced_at"``.
            col_dp_ts: Name of the timestamp column in the SCD2 table that
                identifies the version start (e.g. ``"dp_ts_from"``).  Each call
                to :meth:`merge_into_scd2_table` uses this column to track the
                validity period of each version.  Defaults to ``"dp_ts_from"``, if passed "" explicitly, then the value of col_dp_ts_filter is used.
            col_dp_ts_filter: Name of the timestamp column in the raw table that
                identifies the load batch (e.g. ``"dp_loaded_at"``).  Each call
                to :meth:`merge_into_scd2_table` filters the raw table on
                ``col_dp_ts_filter = dp_ts``.  Defaults to ``"dp_ts"``.
        """
        self.scd2_intermediary_table_name = scd2_intermediary_table_name
        self.cols_bks = cols_bks
        self.cols_val = cols_val
        self.use_logical_delete_for_source_table = use_logical_delete_for_source_table
        self.logical_delete_expression = logical_delete_expression
        self.check_physical_delete_against_source_table = check_physical_delete_against_source_table
        self.materialize_data_before_merge = materialize_data_before_merge
        self.perform_merge_op = perform_merge_op
        self.col_dp_valid_from = col_dp_valid_from
        self.col_dp_valid_to = col_dp_valid_to
        self.col_dp_created_at = col_dp_created_at
        self.col_dp_replaced_at = col_dp_replaced_at
        self.col_dp_ts = col_dp_ts if col_dp_ts else col_dp_ts_filter
        self.col_dp_ts_filter = col_dp_ts_filter

        self._validate_parameters()

    def _validate_parameters(self) -> None:
        """Validate the combination of constructor parameters.

        Raises:
            ValueError: If ``use_logical_delete_for_source_table`` is ``True``
                but ``logical_delete_expression`` is ``None`` or empty — the
                engine has no way to identify deleted records in the raw table.
            ValueError: If ``use_logical_delete_for_source_table`` is ``False``
                (full-snapshot mode) but ``col_dp_ts`` and ``col_dp_ts_filter``
                differ — in full-snapshot mode both columns must refer to the
                same load-batch timestamp so that the filter and the version
                column stay in sync.
        """
        if self.use_logical_delete_for_source_table and not self.logical_delete_expression:
            raise ValueError(
                "logical_delete_expression must be provided when "
                "use_logical_delete_for_source_table is True."
            )
        
        # Warning and auto-fix for inconsistent timestamp column configuration in full-snapshot mode
        if not self.use_logical_delete_for_source_table and self.col_dp_ts != self.col_dp_ts_filter:
#            self.col_dp_ts = self.col_dp_ts_filter  # Force them to be the same for consistency
            logger.warning(
                f"col_dp_ts ('{self.col_dp_ts}') and col_dp_ts_filter ('{self.col_dp_ts_filter}') "
                "must be the same column when use_logical_delete_for_source_table is False "
                "(full-snapshot mode). Automatically fixed col_dp_ts to match col_dp_ts_filter."
            )

    # ── Shared utility methods ──────────────────────────────────────────────

    @staticmethod
    def add_prefix(values: list, prefix: str) -> list:
        """Prepend a table/alias qualifier to each column name.

        Args:
            values: Column names to qualify (e.g. ``["id", "first_name"]``).
            prefix: Table alias or name to prepend (e.g. ``"src"``).
                Pass an empty string or ``None`` to return the list unchanged.

        Returns:
            A new list where every element is ``"<prefix>.<column>"``.
        """
        if prefix:
            return [f"{prefix}.{v}" for v in values]
        return values

    @staticmethod
    def format_values(values: list) -> str:
        """Join a list of column expressions into a comma-separated SQL fragment.

        Args:
            values: Arbitrary column expressions or names.

        Returns:
            A single string of the form ``"expr1, expr2, ..."`` suitable for
            embedding in a SELECT list or similar SQL clause.
        """
        return ", ".join(str(v) for v in values)

    @staticmethod
    def delete_s3_location(s3_client, bucket: str, path: str) -> None:
        """Delete all S3 objects under the given prefix.

        Used before (re)creating an Iceberg table to ensure no stale data files
        remain under the table's S3 location, which could confuse the Iceberg
        metadata layer.

        Args:
            s3_client: A boto3 S3 client (or compatible) used for the deletion.
            bucket: S3 bucket name (without the ``s3a://`` prefix).
            path: Key prefix to delete (e.g. ``"warehouse/default/dim_person"``).
                All objects whose key starts with this prefix are removed.
        """
        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket, Prefix=path)
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
            logger.info(f"S3 folder s3a://{bucket}/{path} deleted successfully.")
        except Exception as e:
            logger.warning(f"Failed to delete S3 folder: {e}")

    def source_table_fqn(self, iceberg_meta_tablename: str = None) -> str:
        """Return the engine-qualified name for the raw staging table.

        Args:
            iceberg_meta_tablename: Optional Iceberg metadata table suffix such as
                ``"snapshots"`` or ``"files"``.  When provided the returned name
                addresses that metadata table (e.g. ``catalog.schema."raw_person$snapshots"``
                for Trino, ``default.raw_person.snapshots`` for Spark).

        Returns:
            Fully-qualified table name ready for use in SQL statements.
        """
        if iceberg_meta_tablename:
            return self._fqn(f'"{self.source_table_name}{self._iceberg_meta_table_sep()}{iceberg_meta_tablename}"')
        else:
            return self._fqn(self.source_table_name)

    def scd2_table_fqn(self, iceberg_meta_tablename: str = None) -> str:
        """Return the engine-qualified name for the SCD2 dimension table.

        Args:
            iceberg_meta_tablename: Optional Iceberg metadata table suffix
                (see :meth:`source_table_fqn` for details).

        Returns:
            Fully-qualified table name ready for use in SQL statements.
        """
        if iceberg_meta_tablename:
            return self._fqn(f'"{self.scd2_table_name}{self._iceberg_meta_table_sep()}{iceberg_meta_tablename}"')
        else:
            return self._fqn(self.scd2_table_name)

    def scd2_intermediary_table_fqn(self, iceberg_meta_tablename: str = None) -> str:
        """Return the engine-qualified name for the intermediary staging table or view.

        Args:
            iceberg_meta_tablename: Optional Iceberg metadata table suffix
                (see :meth:`source_table_fqn` for details).

        Returns:
            Fully-qualified name ready for use in SQL statements.
        """
        if iceberg_meta_tablename:
            return self._fqn(
                f'"{self.scd2_intermediary_table_name}{self._iceberg_meta_table_sep()}{iceberg_meta_tablename}"'
            )
        else:
            return self._fqn(self.scd2_intermediary_table_name)

    def _resolve_table_fqn(self, table: SCD2Table, iceberg_meta_tablename: str = None) -> str:
        """Dispatch a :class:`SCD2Table` enum value to the correct FQN helper.

        Args:
            table: Which logical table to resolve.
            iceberg_meta_tablename: Optional Iceberg metadata table suffix passed
                through to the underlying FQN helper.

        Returns:
            Fully-qualified table name.

        Raises:
            ValueError: If ``table`` is not a recognized :class:`SCD2Table` member.
        """
        if table == SCD2Table.RAW:
            return self.source_table_fqn(iceberg_meta_tablename=iceberg_meta_tablename)
        if table == SCD2Table.SCD2:
            return self.scd2_table_fqn(iceberg_meta_tablename=iceberg_meta_tablename)
        if table == SCD2Table.INTERMEDIARY:
            return self.scd2_intermediary_table_fqn(iceberg_meta_tablename=iceberg_meta_tablename)
        raise ValueError(f"Unknown SCD2Table value: {table}")

    # ── Abstract SQL formatters ─────────────────────────────────────────────

    @abstractmethod
    def _iceberg_meta_table_sep(self) -> str:
        """Return the separator between a table name and an Iceberg metadata suffix.

        Trino uses ``"$"`` (e.g. ``table$snapshots``); Spark uses ``"."``
        (e.g. ``table.snapshots``).
        """

    @abstractmethod
    def _fqn(self, object_name: str) -> str:
        """Return the engine-specific fully-qualified name for a database object.

        Trino format: ``catalog.schema.object``
        Spark format: ``database.object``

        Args:
            object_name: Unqualified object name (table, view, …).

        Returns:
            Fully-qualified name string.
        """

    @abstractmethod
    def format_view(
        self,
        dp_ts: datetime,
    ) -> str:
        """Return the SQL statement that builds the intermediary SCD2 staging view.

        The generated SQL contains a chain of CTEs that:

        1. Reads and hashes source records from the raw table for the given
           ``dp_ts`` batch (filtered on ``self.col_dp_ts_filter``).
        2. FULL OUTER JOINs each source record against the current, previous,
           and next SCD2 versions to detect overlaps and gaps.
        3. Classifies each record as NEW, CHANGED, DELETED, or UNCHANGED.
        4. Expands CHANGED records into two rows using the ``merge_key = NULL``
           trick so that a single MERGE statement can both close the old version
           and insert the new one.

        The resulting view is used as the MERGE source in :meth:`format_merge`.

        Args:
            dp_ts: Timestamp that identifies the raw-table batch to process.
                Only rows where ``<col_dp_ts_filter> = dp_ts`` are read.

        Returns:
            A ``CREATE OR REPLACE VIEW … AS …`` SQL string (Trino) or the raw
            CTE + SELECT SQL to be registered as a temp view (Spark).
        """

    @abstractmethod
    def format_merge(
        self,
        source_view_name: str,
        current_ts: datetime,
    ) -> str:
        """Return the ``MERGE INTO`` statement that applies SCD2 changes.

        The MERGE matches rows from the intermediary staging view against the
        SCD2 dimension table on ``merge_key = dp_record_id``:

        * **Matched rows** (``merge_key`` is not NULL) → UPDATE: closes the
          existing version by setting ``dp_ts_to``, ``dp_is_active``, and
          ``dp_is_latest``.
        * **Unmatched rows** (``merge_key`` is NULL) → INSERT: writes a new
          version with ``dp_ts_from``, ``dp_ts_to = MAX_TS``, a new ``dp_record_id``
          UUID, and the current SHA-256 ``dp_record_hash``.

        Args:
            current_ts: Wall-clock timestamp used as ``dp_created_at`` for newly
                inserted version rows.

        Returns:
            A ``MERGE INTO <scd2_table> USING <intermediary> …`` SQL string.
        """

    # ── Abstract operations ─────────────────────────────────────────────────

    @abstractmethod
    def merge_into_scd2_table(
        self,
        dp_ts: datetime,
        current_ts: Optional[datetime] = None,
        show_input_to_merge: bool = False,
        output_file_name: Optional[str] = None,
    ):
        """Execute the full SCD2 merge pipeline for one load batch.

        Orchestrates the complete view-then-merge workflow:

        1. Calls :meth:`format_view` and registers the intermediary staging view.
        2. If ``self.materialize_data_before_merge`` is ``True``, writes the view
           to a physical table to avoid non-deterministic function issues in MERGE.
        3. Optionally renders the staged records for debugging
           (``show_input_to_merge``).
        4. If ``self.perform_merge_op`` is ``True``, calls :meth:`format_merge`
           and executes the MERGE statement against the SCD2 table.

        Args:
            dp_ts: Timestamp identifying the raw-table batch to process.
                Passed through to :meth:`format_view`.
            current_ts: Wall-clock timestamp used as ``dp_created_at`` for new
                version rows.  Defaults to ``None`` (implementations may default
                to ``datetime.now()``).
            show_input_to_merge: When ``True``, the contents of the intermediary
                staging view are printed/rendered before the MERGE executes.
                Useful for debugging.
            output_file_name: Optional path for a Markdown report file.  When
                provided, rendered output is appended to this file.

        Returns:
        """

    @abstractmethod
    def merge_into_scd2_table_and_return_as_df(
        self,
        dp_ts: datetime,
        current_ts: Optional[datetime] = None,
        show_input_to_merge: bool = False,
        output_file_name: Optional[str] = None,
    ) -> DataFrame:
        """Execute the full SCD2 merge pipeline for one load batch.

        Orchestrates the complete view-then-merge workflow:

        1. Calls :meth:`format_view` and registers the intermediary staging view.
        2. If ``self.materialize_data_before_merge`` is ``True``, writes the view
           to a physical table to avoid non-deterministic function issues in MERGE.
        3. Optionally renders the staged records for debugging
           (``show_input_to_merge``).
        4. If ``self.perform_merge_op`` is ``True``, calls :meth:`format_merge`
           and executes the MERGE statement against the SCD2 table.

        Args:
            dp_ts: Timestamp identifying the raw-table batch to process.
                Passed through to :meth:`format_view`.
            current_ts: Wall-clock timestamp used as ``dp_created_at`` for new
                version rows.  Defaults to ``None`` (implementations may default
                to ``datetime.now()``).
            show_input_to_merge: When ``True``, the contents of the intermediary
                staging view are printed/rendered before the MERGE executes.
                Useful for debugging.
            output_file_name: Optional path for a Markdown report file.  When
                provided, rendered output is appended to this file.

        Returns:
        """

    @abstractmethod
    def get_table_data(
        self,
        table: SCD2Table,
        iceberg_meta_tablename: str = None,
        exclude_cols: list = [],
        order_by_cols: list = [],
        for_version: str = None,
    ) -> "pd.DataFrame":
        """Read one of the three logical SCD2 tables and return it as a DataFrame.

        Args:
            table: Which logical table to read — use the :class:`SCD2Table` enum
                (``RAW``, ``SCD2``, or ``INTERMEDIARY``).
            iceberg_meta_tablename: Optional Iceberg metadata table suffix (e.g.
                ``"snapshots"``, ``"files"``) to query instead of the data table.
            exclude_cols: Column names to drop from the result before returning.
                Useful for omitting hash columns or metadata not relevant to
                test assertions.
            order_by_cols: Columns to sort the result by (ascending).  Providing
                a stable sort order makes test assertions deterministic.
            for_version: Optional Iceberg snapshot identifier for time-travel
                queries.  Trino syntax: ``FOR VERSION AS OF <id>``;
                Spark syntax: ``VERSION AS OF <id>``.

        Returns:
            A pandas DataFrame containing the requested table data.
        """
