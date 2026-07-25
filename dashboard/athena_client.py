"""
=============================================================================
ATHENA CLIENT — reusable query helper
=============================================================================

WHY A SEPARATE MODULE?
  The original lab's app.py inlined Athena query logic alongside Streamlit
  UI code. That mixes two unrelated concerns: data fetching and presentation.

  Splitting them gives us:
    - Reusability: any other tool can import this module
    - Testability: we can unit-test the query lifecycle without Streamlit
    - Readability: app.py stops being 200 lines of intertwined logic

ATHENA QUERY LIFECYCLE:
  Athena is async. Every query goes through three steps:
    1. SUBMIT  — POST the SQL, get back a QueryExecutionId
    2. POLL    — check status every ~0.5s until SUCCEEDED/FAILED/CANCELLED
    3. FETCH   — download results in pages of up to 1,000 rows

  This module wraps all three into one synchronous-feeling function.

=============================================================================
"""

import time
import logging
from typing import Optional
import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG
# Pulled at import time. In production these would be env vars; for the lab
# we keep them centralised here so app.py doesn't have to know AWS details.
# ---------------------------------------------------------------------------
REGION              = "us-east-1"
DATABASE            = "mobile_network_aggregations"
QUERY_OUTPUT_S3     = "s3://roy-projects/mobile-network-lab/athena-results/"

# Polling settings
POLL_INTERVAL_SEC   = 0.5      # how often to check query status
QUERY_TIMEOUT_SEC   = 60       # give up if a query takes longer than this

# Result paging
PAGE_SIZE           = 1000     # max rows per Athena GetQueryResults call
MAX_TOTAL_ROWS      = 10_000   # safety cap — dashboard shouldn't pull more

# ---------------------------------------------------------------------------
# CLIENT (initialised once, reused for all calls)
# ---------------------------------------------------------------------------
# WHY MODULE-LEVEL CLIENT?
# boto3 clients are thread-safe and expensive to construct (they negotiate
# credentials, load endpoints, etc.). Creating one client per query would
# add ~100ms of overhead each time. One module-level client is the standard
# pattern.
_athena = boto3.client("athena", region_name=REGION)


def run_query(sql: str) -> pd.DataFrame:
    """
    Execute an Athena SQL query and return the result as a pandas DataFrame.

    Walks the full submit → poll → fetch lifecycle and converts the
    response into typed columns. Raises RuntimeError on query failure
    or timeout, so callers can surface a meaningful error to the user.

    Parameters
    ----------
    sql : str
        The SQL statement to execute. Must reference fully-qualified
        table names (database.table) or set QueryExecutionContext.

    Returns
    -------
    pd.DataFrame
        Query results with column names matching the SELECT clause.
        Empty DataFrame if the query returned no rows.
    """
    query_id = _submit(sql)
    _wait_for_completion(query_id, sql)
    return _fetch_results(query_id)


# ---------------------------------------------------------------------------
# STEP 1 — SUBMIT
# ---------------------------------------------------------------------------

def _submit(sql: str) -> str:
    """Submit the query and return its execution ID."""
    try:
        response = _athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DATABASE},
            ResultConfiguration={"OutputLocation": QUERY_OUTPUT_S3},
        )
        query_id = response["QueryExecutionId"]
        logger.info(f"Athena query submitted: {query_id}")
        return query_id
    except ClientError as e:
        # Common failures here: bad SQL syntax (caught later by polling),
        # missing IAM permissions, or invalid S3 output location.
        raise RuntimeError(f"Failed to submit Athena query: {e}") from e


# ---------------------------------------------------------------------------
# STEP 2 — POLL
# ---------------------------------------------------------------------------

def _wait_for_completion(query_id: str, sql_for_error: str) -> None:
    """
    Poll Athena until the query reaches a terminal state.

    WHY EXPONENTIAL POLLING ISN'T USED HERE:
    Athena queries typically complete in 1-5 seconds. A fixed 0.5s interval
    means at most ~2 wasted polls before completion, which is fine. We use
    a flat interval rather than exponential backoff because we want low
    latency for fast queries (the common case), not lower API call volume.
    """
    deadline = time.time() + QUERY_TIMEOUT_SEC

    while True:
        if time.time() > deadline:
            _athena.stop_query_execution(QueryExecutionId=query_id)
            raise RuntimeError(
                f"Athena query timed out after {QUERY_TIMEOUT_SEC}s: {query_id}"
            )

        execution = _athena.get_query_execution(QueryExecutionId=query_id)
        status = execution["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            return
        if status in ("FAILED", "CANCELLED"):
            reason = execution["QueryExecution"]["Status"].get(
                "StateChangeReason", "no reason given"
            )
            # Include the offending SQL so devs can debug from logs alone
            raise RuntimeError(
                f"Athena query {status.lower()}: {reason}\n"
                f"Query was:\n{sql_for_error}"
            )

        time.sleep(POLL_INTERVAL_SEC)


# ---------------------------------------------------------------------------
# STEP 3 — FETCH
# ---------------------------------------------------------------------------

def _fetch_results(query_id: str) -> pd.DataFrame:
    """
    Pull all result rows for a completed query and return as DataFrame.

    HOW PAGING WORKS:
    Athena's GetQueryResults returns up to 1,000 rows per call and provides
    a NextToken if more exist. We loop until the token disappears, building
    up the full result set. The MAX_TOTAL_ROWS cap is a safety belt against
    accidentally pulling a million-row scan into Streamlit's memory.

    The first page's first row is the column header — Athena returns column
    names as data, which is a historical oddity of the API. We strip it.
    """
    all_rows: list[list[Optional[str]]] = []
    column_names: Optional[list[str]] = None
    next_token: Optional[str] = None

    while True:
        kwargs = {"QueryExecutionId": query_id, "MaxResults": PAGE_SIZE}
        if next_token:
            kwargs["NextToken"] = next_token

        page = _athena.get_query_results(**kwargs)

        # Extract column names from the metadata on the first page only
        if column_names is None:
            column_names = [
                col["Name"]
                for col in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
            ]
            # Athena's first data row is actually the header — skip it
            data_rows = page["ResultSet"]["Rows"][1:]
        else:
            data_rows = page["ResultSet"]["Rows"]

        # Convert each row to a flat list of cell values (or None for nulls)
        for row in data_rows:
            all_rows.append([cell.get("VarCharValue") for cell in row["Data"]])

        if len(all_rows) >= MAX_TOTAL_ROWS:
            logger.warning(
                f"Result truncated at {MAX_TOTAL_ROWS} rows. "
                "Add a LIMIT clause or filter to your query."
            )
            break

        next_token = page.get("NextToken")
        if not next_token:
            break

    df = pd.DataFrame(all_rows, columns=column_names or [])
    logger.info(f"Athena query {query_id} returned {len(df)} rows")
    return df


# ---------------------------------------------------------------------------
# TYPE COERCION HELPER
# ---------------------------------------------------------------------------
# Athena returns every cell as a string regardless of the column's actual
# type. We coerce numeric and timestamp columns here so the dashboard can
# work with them naturally (sorting, formatting, math).
#
# WHY NOT JUST USE pd.read_sql_query?
# That requires a SQLAlchemy connection to Athena, which is an extra
# dependency (pyathena or similar). Sticking to boto3 keeps the deployment
# image leaner and avoids one more thing that can go wrong in production.

def coerce_types(df: pd.DataFrame, numeric_cols: list[str] = None,
                 timestamp_cols: list[str] = None) -> pd.DataFrame:
    """
    Convert string columns from Athena into numeric/timestamp dtypes.

    Errors during conversion become NaN (for numeric) or NaT (for timestamp)
    rather than raising, so a single bad row doesn't crash the dashboard.
    """
    df = df.copy()
    for col in (numeric_cols or []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in (timestamp_cols or []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df