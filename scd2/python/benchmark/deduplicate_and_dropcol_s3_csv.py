"""
Reads CSV files from S3 under a folder structure like:

    <s3_prefix>/<folder_name>/<date>/xxxx.csv

Deduplicates each file by one or more key columns (keeping the last occurrence) and optionally drops specified columns,
then writes the result back to S3 as a new CSV under an output prefix that mirrors
the same folder/date structure.

Usage
-----
    python deduplicate_and_dropcol_s3_csv.py \
        --bucket upload-bucket \
        --prefix raw_data \
        --keys id \
        --keys version \      
        --output-prefix raw_data_deduped \
        [--keep first|last]          # which duplicate to keep (default: last)
        [--endpoint http://localhost:9000]
        [--drop-cols col1 --drop-cols col2 ...]  # columns to drop from the output
        [--dry-run]                 # list files and keys without writing output
"""

import argparse
import fnmatch
import io
import logging
import os
import sys

import boto3
import pandas as pd
from botocore.client import Config

from benchmark_commons import get_param, get_credential

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# S3 helpers
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


def read_csv_from_s3(s3, bucket: str, key: str) -> pd.DataFrame:
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(response["Body"].read()))


def write_csv_to_s3(s3, df: pd.DataFrame, bucket: str, key: str) -> None:
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    logger.info(f"Written {len(df)} rows → s3://{bucket}/{key}")


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def deduplicate_file(
    s3,
    bucket: str,
    key: str,
    keys: list[str],
    keep: str,
    output_prefix: str,
    input_prefix: str,
    drop_cols: list[str] = None,
) -> dict:
    """Deduplicate one CSV file and write the result to output_prefix."""
    df = read_csv_from_s3(s3, bucket, key)
    rows_before = len(df)

    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise ValueError(f"Key column(s) {missing} not found in {key}. Available: {list(df.columns)}")

    if drop_cols:
        unknown = [c for c in drop_cols if c not in df.columns]
        if unknown:
            logger.warning(f"Column(s) {unknown} not found in {key} — skipping those drops.")
        df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    df_deduped = df.drop_duplicates(subset=keys, keep=keep)
    rows_after = len(df_deduped)

    # Mirror the path: replace input_prefix with output_prefix
    relative_path = key[len(input_prefix):].lstrip("/")
    output_key = f"{output_prefix.rstrip('/')}/{relative_path}"

    write_csv_to_s3(s3, df_deduped, bucket, output_key)

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
    # For path mirroring, strip at the wildcard boundary so relative paths are correct
    input_prefix = _wildcard_base(prefix) if "*" in prefix else prefix

    csv_keys = list_csv_keys(s3, bucket, prefix)
    if not csv_keys:
        logger.warning(f"No CSV files found under s3://{bucket}/{prefix}")
        return []

    logger.info(f"Found {len(csv_keys)} CSV file(s) under s3://{bucket}/{prefix}")

    results = []
    for key in csv_keys:
        logger.info(f"Processing s3://{bucket}/{key} ...")
        if dry_run:
            logger.info(f"  [dry-run] would deduplicate by keys={keys}, keep={keep}, drop_cols={drop_cols}")
            continue
        result = deduplicate_file(
            s3=s3,
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

    total_removed = sum(r["duplicates_removed"] for r in results)
    logger.info(f"Done. Processed {len(results)} file(s), removed {total_removed} duplicate rows in total.")
    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deduplicate CSV files stored in S3 under folder_name/<date>/xxxx.csv"
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
