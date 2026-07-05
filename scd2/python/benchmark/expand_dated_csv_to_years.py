"""
Expand a sparse set of dated CSV folders into n years of weekday-only data.

Source layout:   <bucket>/<prefix>/YYYYmmdd/<files>
Output layout:   <bucket>/<output-prefix>/YYYYmmdd/<files>

For each weekday in the target date range, one source folder is picked in
ascending order (cycling back to the first when exhausted) and all its files
are copied to the output folder named after the TARGET date.  Saturdays and
Sundays are always skipped — no output folder is created for them.

Usage
-----
    python expand_dated_csv_to_years.py \\
        --bucket upload-bucket \\
        --prefix data \\
        --output-prefix datagen \\
        --years 3 \\
        [--start-date 2024-01-01]   # default: today
        [--if-exists overwrite|skip]
        [--endpoint http://localhost:9000]
        [--dry-run]
"""

import argparse
import logging
import re
from datetime import date, timedelta
from itertools import cycle

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from benchmark_commons import get_param

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DATE_FOLDER_RE = re.compile(r"^\d{8}$")


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _s3_client(endpoint_url: str, access_key: str, secret_key: str) -> boto3.client:
    if not access_key or not secret_key:
        raise ValueError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set "
            "(via env vars or --access-key / --secret-key flags)"
        )
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url or None,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _s3_key_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def list_dated_folders(s3, bucket: str, prefix: str) -> list[tuple[str, list[str]]]:
    """Return sorted [(date_str, [keys])] for every YYYYmmdd subfolder under prefix."""
    prefix = prefix.rstrip("/") + "/"
    paginator = s3.get_paginator("list_objects_v2")

    folders: dict[str, list[str]] = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folder_prefix = cp["Prefix"]               # e.g. 'data/20260605/'
            folder_name = folder_prefix.rstrip("/").split("/")[-1]
            if not _DATE_FOLDER_RE.match(folder_name):
                continue

            keys = []
            for fp in paginator.paginate(Bucket=bucket, Prefix=folder_prefix):
                for obj in fp.get("Contents", []):
                    key = obj["Key"]
                    filename = key.split("/")[-1]
                    if folder_name in filename:
                        keys.append(key)

            if keys:
                folders[folder_name] = sorted(keys)

    return sorted(folders.items())   # ascending by date string


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date '{s}' — expected YYYY-MM-DD")


def _weekdays(start: date, years: int):
    """Yield every Mon–Fri date in [start, start + n_years)."""
    end = date(start.year + years, start.month, start.day)
    d = start
    while d < end:
        if d.weekday() < 5:     # 0=Mon … 4=Fri
            yield d
        d += timedelta(days=1)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def run(
    bucket: str,
    prefix: str,
    output_prefix: str,
    years: int,
    start_date: date,
    endpoint_url: str = None,
    access_key: str = None,
    secret_key: str = None,
    dry_run: bool = False,
    if_exists: str = "overwrite",
) -> None:
    s3 = _s3_client(endpoint_url, access_key, secret_key)

    logger.info(f"Listing dated folders under s3://{bucket}/{prefix.rstrip('/')}/")
    dated = list_dated_folders(s3, bucket, prefix)
    if not dated:
        logger.error(f"No YYYYmmdd folders found under s3://{bucket}/{prefix}/")
        return

    logger.info(f"Found {len(dated)} source folder(s): {[d for d, _ in dated]}")

    src_prefix = prefix.rstrip("/") + "/"
    out_prefix = output_prefix.rstrip("/")
    source_cycle = cycle(dated)

    copied = skipped = 0
    target_days = list(_weekdays(start_date, years))
    logger.info(f"Generating {len(target_days)} weekday target dates over {years} year(s) from {start_date}")

    for target_date in target_days:
        date_str = target_date.strftime("%Y%m%d")
        src_date, src_keys = next(source_cycle)

        for src_key in src_keys:
            # Strip source folder prefix to get the bare filename(s)
            rel = src_key[len(src_prefix) + len(src_date) + 1:]   # skip 'prefix/YYYYmmdd/'
            dst_key = f"{out_prefix}/{date_str}/{rel}"

            if dry_run:
                logger.info(f"  [dry-run] {src_key} → {dst_key}")
                continue

            if if_exists == "skip" and _s3_key_exists(s3, bucket, dst_key):
                logger.debug(f"  Skip (exists): s3://{bucket}/{dst_key}")
                skipped += 1
                continue

            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": src_key},
                Key=dst_key,
            )
            logger.info(f"  Copied s3://{bucket}/{src_key} → s3://{bucket}/{dst_key}")
            copied += 1

    if not dry_run:
        logger.info(f"Done. Copied: {copied} file(s), skipped: {skipped} file(s).")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Expand sparse dated CSV folders to n years of weekday data on S3."
    )
    parser.add_argument("--bucket", default=get_param("S3_UPLOAD_BUCKET", "upload-bucket"),
                        help="S3 bucket name")
    parser.add_argument("--prefix", default="data",
                        help="Source prefix containing YYYYmmdd subfolders (default: data)")
    parser.add_argument("--output-prefix", default="datagen",
                        help="Output prefix for generated folders (default: datagen)")
    parser.add_argument("--years", type=int, required=True,
                        help="Number of years of weekday data to generate")
    parser.add_argument("--start-date", type=_parse_date, default=date.today(),
                        metavar="YYYY-MM-DD",
                        help="First date of the generated range (default: today)")
    parser.add_argument("--if-exists", choices=["overwrite", "skip"], default="overwrite",
                        help="What to do when an output file already exists (default: overwrite)")
    parser.add_argument("--endpoint", default=get_param("S3_ENDPOINT_URL", "http://localhost:9000"),
                        help="S3 endpoint URL (for MinIO / non-AWS)")
    parser.add_argument("--access-key", default=get_param("AWS_ACCESS_KEY_ID", None),
                        help="S3 access key (default: AWS_ACCESS_KEY_ID env var)")
    parser.add_argument("--secret-key", default=get_param("AWS_SECRET_ACCESS_KEY", None),
                        help="S3 secret key (default: AWS_SECRET_ACCESS_KEY env var)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be copied without writing anything")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(
        bucket=args.bucket,
        prefix=args.prefix,
        output_prefix=args.output_prefix,
        years=args.years,
        start_date=args.start_date,
        endpoint_url=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        dry_run=args.dry_run,
        if_exists=args.if_exists,
    )
