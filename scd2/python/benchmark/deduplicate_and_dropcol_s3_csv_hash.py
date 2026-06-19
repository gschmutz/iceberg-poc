"""
Hash-based deduplication of CSV files stored in S3.

Streams each file row-by-row so only a set of 16-byte hashes is held in
memory — suitable for very large files.  The first occurrence of each key
is kept; subsequent duplicates are discarded.

Folder structure expected:

    <s3_prefix>/<folder_name>/<date>/xxxx.csv

Usage
-----
    python deduplicate_and_dropcol_s3_csv_hash.py \\
        --bucket upload-bucket \\
        --prefix raw_data \\
        --keys id \\
        --keys version \\
        --output-prefix raw_data_deduped \\
        [--drop-cols col1 --drop-cols col2] \\
        [--endpoint http://localhost:9000] \\
        [--dry-run]
"""

import argparse
import csv
import hashlib
import io
import logging
import tempfile

import boto3
from botocore.client import Config

from benchmark_commons import get_credential, get_param

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


def list_csv_keys(s3, bucket: str, prefix: str) -> list[str]:
    """Return all object keys under prefix that end with .csv."""
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                keys.append(obj["Key"])
    return keys


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def deduplicate_file(
    s3,
    bucket: str,
    key: str,
    keys: list[str],
    output_prefix: str,
    input_prefix: str,
    drop_cols: list[str] = None,
) -> dict:
    """
    Stream a CSV from S3, deduplicate by hashing key columns (keeping first),
    drop any requested columns, and upload the result back to S3.
    """
    drop_cols = drop_cols or []

    # Stream input from S3 — wrapping the binary StreamingBody as text
    response = s3.get_object(Bucket=bucket, Key=key)
    stream = io.TextIOWrapper(response["Body"], encoding="utf-8", newline="")
    reader = csv.DictReader(stream)

    missing = [k for k in keys if k not in (reader.fieldnames or [])]
    if missing:
        raise ValueError(
            f"Key column(s) {missing} not found in {key}. Available: {reader.fieldnames}"
        )

    unknown_drops = [c for c in drop_cols if c not in (reader.fieldnames or [])]
    if unknown_drops:
        logger.warning(f"Column(s) {unknown_drops} not found in {key} — skipping those drops.")
    drop_cols = [c for c in drop_cols if c in (reader.fieldnames or [])]

    out_fields = [f for f in reader.fieldnames if f not in drop_cols]

    seen_hashes: set[bytes] = set()
    rows_before = 0
    duplicates_removed = 0

    # Write deduplicated output to a temp file to avoid holding everything in memory
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8", newline="") as tmp:
        writer = csv.DictWriter(tmp, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()

        for row in reader:
            rows_before += 1
            key_value = "|".join(str(row[k]) for k in keys).encode()
            row_hash = hashlib.blake2b(key_value, digest_size=16).digest()

            if row_hash not in seen_hashes:
                seen_hashes.add(row_hash)
                writer.writerow(row)
            else:
                duplicates_removed += 1

        rows_after = rows_before - duplicates_removed

        # Upload temp file to S3
        tmp.seek(0)
        relative_path = key[len(input_prefix):].lstrip("/")
        output_key = f"{output_prefix.rstrip('/')}/{relative_path}"
        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=tmp.read().encode("utf-8"),
        )
        logger.info(f"Written {rows_after} rows → s3://{bucket}/{output_key}")

    return {
        "input_key": key,
        "output_key": output_key,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "duplicates_removed": duplicates_removed,
    }


def run(
    bucket: str,
    prefix: str,
    keys: list[str],
    output_prefix: str,
    drop_cols: list[str] = None,
    endpoint_url: str = None,
    dry_run: bool = False,
) -> list[dict]:
    s3 = _s3_client(endpoint_url)

    csv_keys = list_csv_keys(s3, bucket, prefix)
    if not csv_keys:
        logger.warning(f"No CSV files found under s3://{bucket}/{prefix}")
        return []

    logger.info(f"Found {len(csv_keys)} CSV file(s) under s3://{bucket}/{prefix}")

    if dry_run:
        for key in csv_keys:
            logger.info(f"  [dry-run] s3://{bucket}/{key} — keys={keys}, drop_cols={drop_cols}")
        return []

    results = []
    for key in csv_keys:
        logger.info(f"Processing s3://{bucket}/{key} ...")
        result = deduplicate_file(
            s3=s3,
            bucket=bucket,
            key=key,
            keys=keys,
            output_prefix=output_prefix,
            input_prefix=prefix,
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
        description="Hash-based CSV deduplication on S3 (streams row-by-row, keeps first occurrence)"
    )
    parser.add_argument("--bucket", default=get_param("S3_UPLOAD_BUCKET", "upload-bucket"),
                        help="S3 bucket name")
    parser.add_argument("--prefix", required=True,
                        help="S3 key prefix to scan, e.g. 'raw_data/customers'")
    parser.add_argument("--keys", required=True, action="append", dest="keys",
                        help="Deduplication key column (repeat for composite keys)")
    parser.add_argument("--output-prefix",
                        help="S3 prefix for deduplicated output (default: <prefix>_deduped)")
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
        drop_cols=args.drop_cols or None,
        endpoint_url=args.endpoint,
        dry_run=args.dry_run,
    )
