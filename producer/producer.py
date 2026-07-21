"""
STAGE 1 — Kinesis Producer

WHAT THIS FILE DOES:
  Reads mobile-logs1.csv and ingests every
  row into a Kinesis data stream using batched writes with exponential
  backoff retry on throttling.

"""

import json
import csv
import time
import math
import logging
import os
from typing import Generator
import boto3
from botocore.exceptions import ClientError


STREAM_NAME   = "mobile_coverage_logs"
AWS_REGION    = "us-east-1"
CSV_FILE_PATH = "data/mobile-logs1.csv"

# Kinesis limits
MAX_BATCH_SIZE        = 500
MAX_RECORD_SIZE_BYTES = 1_000_000

# Retry settings
MAX_RETRIES    = 5
BASE_DELAY_SEC = 0.5


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kinesis_producer")



def read_csv(file_path: str) -> list[dict]:
    """Read a CSV file and return a list of row dicts."""
    abs_path = os.path.join(os.getcwd(), file_path)
    logger.info(f"Reading CSV from: {abs_path}")

    with open(abs_path, "r") as f:
        rows = list(csv.DictReader(f))

    logger.info(f"Loaded {len(rows):,} rows from CSV")
    return rows


def rows_to_kinesis_records(rows: list[dict]) -> list[dict]:
    """
    Convert CSV row dicts into Kinesis put_records format.

    PARTITION KEY — postal_code:
    Postal codes have hundreds of unique values, giving even shard
    distribution. Records from the same postal code also arrive at
    the consumer in order.
    """
    records = []

    for row in rows:
        payload = json.dumps(row).encode("utf-8")

        if len(payload) > MAX_RECORD_SIZE_BYTES:
            logger.warning(f"Skipping oversized record for postal_code={row.get('postal_code')}")
            continue

        records.append({
            "Data":         payload,
            "PartitionKey": row["postal_code"],
        })

    return records


def chunk(lst: list, size: int) -> Generator:
    """Split a list into chunks of at most `size` items."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def put_records_with_retry(client, stream_name: str, records: list[dict], attempt: int = 0) -> int:
    """
    Send a batch to Kinesis, retrying failed records with exponential backoff.
    Returns the number of records successfully sent.

    HOW KINESIS PARTIAL FAILURES WORK:
    put_records does NOT raise if some records fail. It returns a
    FailedRecordCount and each failed record carries an ErrorCode.
    The two retriable error codes are:
      - ProvisionedThroughputExceededException (shard at capacity)
      - InternalFailure (transient AWS issue)
    We re-send only the failed records with exponential backoff
    (0.5s → 1s → 2s → 4s → 8s).
    """
    try:
        response = client.put_records(StreamName=stream_name, Records=records)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        logger.error(f"ClientError on put_records: {error_code} — {e}")
        raise

    failed_count = response.get("FailedRecordCount", 0)
    if failed_count == 0:
        return len(records)

    if attempt >= MAX_RETRIES:
        logger.error(f"Giving up after {MAX_RETRIES} retries. {failed_count} records lost.")
        return len(records) - failed_count

    delay = BASE_DELAY_SEC * math.pow(2, attempt)
    logger.warning(
        f"{failed_count} records failed (attempt {attempt + 1}/{MAX_RETRIES}). "
        f"Retrying in {delay:.1f}s..."
    )
    time.sleep(delay)

    failed_records = [
        records[i]
        for i, result in enumerate(response["Records"])
        if "ErrorCode" in result
    ]

    return (len(records) - failed_count) + put_records_with_retry(
        client, stream_name, failed_records, attempt + 1
    )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def produce(csv_path: str = CSV_FILE_PATH) -> None:
    client = boto3.client("kinesis", region_name=AWS_REGION)

    rows = read_csv(csv_path)
    records = rows_to_kinesis_records(rows)

    total_sent = 0
    total_batches = math.ceil(len(records) / MAX_BATCH_SIZE)

    logger.info(
        f"Starting ingestion: {len(records):,} records → "
        f"{total_batches} batch(es) of up to {MAX_BATCH_SIZE} records each"
    )

    for batch_num, batch in enumerate(chunk(records, MAX_BATCH_SIZE), start=1):
        sent = put_records_with_retry(client, STREAM_NAME, batch)
        total_sent += sent
        logger.info(f"Batch {batch_num}/{total_batches} — sent {sent}/{len(batch)} records")

    logger.info(f"Ingestion complete. {total_sent:,}/{len(records):,} records sent successfully.")


if __name__ == "__main__":
    produce()