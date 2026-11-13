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
CSV_FILE_PATH = "data/kenya-mobile-logs.csv"

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
        payload = (json.dumps(row) + "\n").encode("utf-8")
        """    
        DELIMITER — trailing "\n":
        Firehose concatenates records byte-for-byte with no separator. Without
        the newline, bronze files become {...}{...}{...} and line-based JSON
        readers (Spark, Athena) parse only the first object per file."""

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

def event_offset_seconds(hhmmss: str) -> int:
    """Seconds since midnight for an 'HH:MM:SS' event time."""
    h, m, s = (int(x) for x in hhmmss.split(":"))
    return h * 3600 + m * 60 + s


def send(client, batch: list[dict]) -> int:
    return put_records_with_retry(client, STREAM_NAME, batch)


def produce(csv_path: str = CSV_FILE_PATH, minutes_per_day: float = 0) -> None:
    """
    Send every CSV row to Kinesis.

    minutes_per_day = 0  → burst: as fast as possible (original behaviour).
    minutes_per_day > 0  → paced replay: the 24 h of event time are spread
                           over this many real minutes, in event-time order.

    WHY PACE: a burst puts a whole day of event time on the stream in seconds.
    The two shards are then read at slightly different speeds, the streaming
    watermark jumps ahead with whichever shard leads, and Spark drops the other
    shard's records as late. On 2026-11-07 that silently lost 4,228 of 19,998
    events. Pacing keeps both shards close in event time and stays far below
    Kinesis limits, so nothing is throttled and retried out of order.
    """
    client = boto3.client("kinesis", region_name=AWS_REGION)

    rows = read_csv(csv_path)
    records = rows_to_kinesis_records(rows)
    pairs = sorted(zip(rows, records), key=lambda rr: rr[0]["hour"])
    total = len(pairs)
    total_sent = 0

    if minutes_per_day <= 0:
        logger.info(f"Burst mode: {total:,} records")
        for batch_num, batch in enumerate(chunk([r for _, r in pairs], MAX_BATCH_SIZE), start=1):
            total_sent += send(client, batch)
        logger.info(f"Ingestion complete. {total_sent:,}/{total:,} records sent.")
        return

    scale = minutes_per_day * 60 / 86_400          # real seconds per event second
    logger.info(
        f"Paced mode: {total:,} records, 24 h of event time over "
        f"{minutes_per_day:g} min ({1 / scale:.0f}x real time)"
    )
    t0 = time.monotonic()
    i = 0
    last_hour = None
    while i < total:
        elapsed = time.monotonic() - t0
        batch = []
        while (i < total and len(batch) < MAX_BATCH_SIZE
               and event_offset_seconds(pairs[i][0]["hour"]) * scale <= elapsed):
            batch.append(pairs[i][1])
            i += 1
        if batch:
            total_sent += send(client, batch)
            hour = pairs[i - 1][0]["hour"][:2]
            if hour != last_hour:
                logger.info(f"event hour {hour}:00 reached — {total_sent:,}/{total:,} sent")
                last_hour = hour
        else:
            due = event_offset_seconds(pairs[i][0]["hour"]) * scale
            time.sleep(min(0.25, max(0.0, due - elapsed)))

    logger.info(
        f"Ingestion complete. {total_sent:,}/{total:,} records sent in "
        f"{(time.monotonic() - t0) / 60:.1f} min."
    )


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Send the CSV to Kinesis.")
    ap.add_argument("--csv", default=CSV_FILE_PATH)
    ap.add_argument("--minutes-per-day", type=float, default=0,
                    help="0 = burst (default). e.g. 24 = one event-hour per real minute.")
    a = ap.parse_args()
    produce(a.csv, a.minutes_per_day)
