"""
DuckDB-based variant of deduplicate_and_dropcol_s3_csv.py.

Reads CSV files from S3 under a folder structure like:

    <s3_prefix>/<folder_name>/<date>/xxxx.csv

Deduplicates each file by one or more key columns and optionally drops columns,
then writes the result back to S3 as a new CSV.  All processing happens inside
DuckDB (no pandas) via the httpfs extension, so large files stay out of memory.

Usage
-----
    python deduplicate_and_dropcol_s3_csv_duckdb.py \\
        --bucket upload-bucket \\
        --prefix raw_data \\
        --keys id \\
        --keys version \\
        --output-prefix raw_data_deduped \\
        [--keep first|last]                   # which duplicate to keep (default: last)
        [--drop-cols col1 --drop-cols col2]   # columns to remove from the output
        [--endpoint http://localhost:9000]
        [--dry-run]
"""

import argparse
import fnmatch
import logging
import re

import boto3
import duckdb
from botocore.client import Config

from benchmark_commons import get_credential, get_param

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# S3 helpers (boto3 used only for listing keys)
# ---------------------------------------------------------------------------

def _s3_client(endpoint_url: str) -> boto3.client:
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url or None,
        aws_access_key_id=get_credential("AWS_ACCESS_KEY_ID", None),
        aws_secret_access_key=get_credential("AWS_SECRET_ACCESS_KEY", None),
        config=Config(signature_version="s3v4"),
    )


def _wildcard_base(prefix: str) -> str:
    """Return the S3-safe listing prefix: everything up to the last '/' before the first '*'."""
    wildcard_pos = prefix.index("*")
    slash_pos = prefix.rfind("/", 0, wildcard_pos)
    return prefix[: slash_pos + 1] if slash_pos >= 0 else ""


def list_csv_keys(s3, bucket: str, prefix: str) -> list[str]:
    """Return all object keys matching prefix (supports * wildcards) that end with .csv."""
    s3_prefix = _wildcard_base(prefix) if "*" in prefix else prefix
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue
            if "*" in prefix and not fnmatch.fnmatch(key, prefix + "*"):
                continue
            keys.append(key)
    return keys


# ---------------------------------------------------------------------------
# DuckDB setup
# ---------------------------------------------------------------------------

def _duckdb_connection(endpoint_url: str) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with httpfs configured for S3 / MinIO."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    access_key = get_credential("AWS_ACCESS_KEY_ID", None)
    secret_key = get_credential("AWS_SECRET_ACCESS_KEY", None)

    if access_key:
        con.execute(f"SET s3_access_key_id='{access_key}';")
    if secret_key:
        con.execute(f"SET s3_secret_access_key='{secret_key}';")

    if endpoint_url:
        # Strip scheme — DuckDB expects just the host[:port]
        host = re.sub(r"^https?://", "", endpoint_url).rstrip("/")
        con.execute(f"SET s3_endpoint='{host}';")
        use_ssl = endpoint_url.startswith("https")
        con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
        con.execute("SET s3_url_style='path';")

    return con


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def _build_dedup_sql(
    s3_url: str,
    keys: list[str],
    keep: str,
    drop_cols: list[str],
) -> str:
    """Build a DuckDB SELECT that deduplicates and drops columns."""
    order_dir = "ASC" if keep == "first" else "DESC"
    partition_by = ", ".join(keys)

    if drop_cols:
        exclude_clause = "EXCLUDE (" + ", ".join(drop_cols) + ")"
    else:
        exclude_clause = ""

    return f"""
    WITH
    src AS (
        SELECT *, ROW_NUMBER() OVER () AS _rn
        FROM read_csv_auto('{s3_url}', header = true)
    ),
    deduped AS (
        SELECT * {exclude_clause}
        FROM src
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY _rn {order_dir}) = 1
    )
    SELECT * FROM deduped
    """


def deduplicate_file(
    con: duckdb.DuckDBPyConnection,
    bucket: str,
    key: str,
    keys: list[str],
    keep: str,
    output_prefix: str,
    input_prefix: str,
    drop_cols: list[str] = None,
) -> dict:
    """Deduplicate one CSV file via DuckDB and write the result back to S3."""
    input_url = f"s3://{bucket}/{key}"

    # Validate key columns exist
    cols_in_file = [
        row[0] for row in con.execute(
            f"SELECT column_name FROM (DESCRIBE SELECT * FROM read_csv_auto('{input_url}', header=true))"
        ).fetchall()
    ]
    missing = [k for k in keys if k not in cols_in_file]
    if missing:
        raise ValueError(f"Key column(s) {missing} not found in {key}. Available: {cols_in_file}")

    if drop_cols:
        unknown = [c for c in drop_cols if c not in cols_in_file]
        if unknown:
            logger.warning(f"Column(s) {unknown} not found in {key} — skipping those drops.")
        drop_cols = [c for c in drop_cols if c in cols_in_file]

    rows_before = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{input_url}', header=true)"
    ).fetchone()[0]

    relative_path = key[len(input_prefix):].lstrip("/")
    output_key = f"{output_prefix.rstrip('/')}/{relative_path}"
    output_url = f"s3://{bucket}/{output_key}"

    dedup_sql = _build_dedup_sql(input_url, keys, keep, drop_cols or [])
    con.execute(f"COPY ({dedup_sql}) TO '{output_url}' (FORMAT CSV, HEADER true);")

    rows_after = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{output_url}', header=true)"
    ).fetchone()[0]

    logger.info(f"Written {rows_after} rows → {output_url}")

    return {
        "input_key": key,
        "output_key": output_key,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "duplicates_removed": rows_before - rows_after,
    }


def run(
    bucket: str,
    prefix: str,
    keys: list[str],
    output_prefix: str,
    keep: str = "last",
    drop_cols: list[str] = None,
    endpoint_url: str = None,
    dry_run: bool = False,
) -> list[dict]:
    s3 = _s3_client(endpoint_url)
    input_prefix = _wildcard_base(prefix) if "*" in prefix else prefix

    csv_keys = list_csv_keys(s3, bucket, prefix)
    if not csv_keys:
        logger.warning(f"No CSV files found under s3://{bucket}/{prefix}")
        return []

    logger.info(f"Found {len(csv_keys)} CSV file(s) under s3://{bucket}/{prefix}")

    if dry_run:
        for key in csv_keys:
            logger.info(f"  [dry-run] s3://{bucket}/{key} — keys={keys}, keep={keep}, drop_cols={drop_cols}")
        return []

    con = _duckdb_connection(endpoint_url)

    results = []
    for key in csv_keys:
        logger.info(f"Processing s3://{bucket}/{key} ...")
        result = deduplicate_file(
            con=con,
            bucket=bucket,
            key=key,
            keys=keys,
            keep=keep,
            output_prefix=output_prefix,
            input_prefix=input_prefix,
            drop_cols=drop_cols,
        )
        results.append(result)
        logger.info(
            f"  {result['rows_before']} → {result['rows_after']} rows "
            f"({result['duplicates_removed']} duplicates removed)"
        )

    con.close()
    total_removed = sum(r["duplicates_removed"] for r in results)
    logger.info(f"Done. Processed {len(results)} file(s), removed {total_removed} duplicate rows in total.")
    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deduplicate CSV files on S3 using DuckDB (folder_name/<date>/xxxx.csv)"
    )
    parser.add_argument("--bucket", default=get_param("S3_UPLOAD_BUCKET", "upload-bucket"),
                        help="S3 bucket name")
    parser.add_argument("--prefix", required=True,
                        help="S3 key prefix to scan, e.g. 'raw_data/customers' or 'raw_data/2024*'")
    parser.add_argument("--keys", required=True, action="append", dest="keys",
                        help="Deduplication key column (repeat for composite keys)")
    parser.add_argument("--output-prefix",
                        help="S3 prefix for deduplicated output (default: <prefix>_deduped)")
    parser.add_argument("--keep", choices=["first", "last"], default="last",
                        help="Which duplicate occurrence to keep (default: last)")
    parser.add_argument("--drop-cols", action="append", dest="drop_cols", default=[],
                        help="Column to remove from the output (repeat for multiple columns)")
    parser.add_argument("--endpoint", default=get_param("S3_ENDPOINT_URL", "http://localhost:9000"),
                        help="S3 endpoint URL (for MinIO / non-AWS)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List files and key columns without writing anything")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    output_prefix = args.output_prefix or f"{args.prefix.rstrip('/')}_deduped"

    run(
        bucket=args.bucket,
        prefix=args.prefix,
        keys=args.keys,
        output_prefix=output_prefix,
        keep=args.keep,
        drop_cols=args.drop_cols or None,
        endpoint_url=args.endpoint,
        dry_run=args.dry_run,
    )
