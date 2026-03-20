"""
conftest.py
-----------
Shared pytest fixtures for the retail pipeline test suite.

A single SparkSession in local mode is created once per test session to keep
test execution fast.  Individual tests receive DataFrames constructed from
plain Python lists, so no external services (PostgreSQL, HDFS) are needed.
"""

from __future__ import annotations

import os
import smtplib
import socket
from datetime import datetime
from email.message import EmailMessage
from typing import List

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Email notification on test failure
# ---------------------------------------------------------------------------
# Reads the same SMTP env vars used by Airflow (set in docker-compose.yml).
# Silently skips if ALERT_EMAIL or SMTP credentials are not configured.


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Send an email alert when any test fails.

    Triggered automatically by pytest after the full test session finishes.
    No-ops when ALERT_EMAIL is unset or SMTP credentials are missing,
    so removing the env vars is sufficient to disable notifications.
    """
    if exitstatus == 0:
        return  # all tests passed — nothing to report

    alert_email = os.getenv("ALERT_EMAIL", "")
    smtp_host = os.getenv("AIRFLOW__SMTP__SMTP_HOST", "")
    smtp_port = int(os.getenv("AIRFLOW__SMTP__SMTP_PORT", "587"))
    smtp_user = os.getenv("AIRFLOW__SMTP__SMTP_USER", "")
    smtp_password = os.getenv("AIRFLOW__SMTP__SMTP_PASSWORD", "")
    smtp_from = os.getenv("AIRFLOW__SMTP__SMTP_MAIL_FROM", smtp_user)
    use_tls = os.getenv("AIRFLOW__SMTP__SMTP_STARTTLS", "true").lower() == "true"

    if not all([alert_email, smtp_host, smtp_user, smtp_password]):
        return  # notifications not configured — skip silently

    failed = getattr(session, "testsfailed", 0)
    host = socket.gethostname()

    msg = EmailMessage()
    msg["Subject"] = f"[TEST FAILURE] {failed} test(s) failed on {host}"
    msg["From"] = smtp_from
    msg["To"] = alert_email
    msg.set_content(
        f"{failed} test(s) failed during the pytest session on host '{host}'.\n\n"
        f"Exit status: {exitstatus}\n"
        f"Total collected: {session.testscollected}\n\n"
        f"Check the container logs for the full traceback:\n"
        f"  docker compose logs tests\n"
        f"  docker compose logs integration-tests\n"
    )

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            if use_tls:
                server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
    except Exception:
        pass  # never let a notification failure break the test exit code


# ---------------------------------------------------------------------------
# SparkSession – created once for the whole test session
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Return a local SparkSession suitable for unit tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("RetailPipelineTests")
        # Suppress most Spark INFO/WARN noise during tests
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Raw (pre-cleaning) schema – mirrors what CSV inferSchema produces
# ---------------------------------------------------------------------------

RAW_SCHEMA = StructType(
    [
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", DoubleType(), True),
        StructField("InvoiceDate", TimestampType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Revenue", DoubleType(), True),
    ]
)

# ---------------------------------------------------------------------------
# Cleaned schema – mirrors what clean_data() produces
# ---------------------------------------------------------------------------

CLEANED_SCHEMA = StructType(
    [
        StructField("invoice_no", StringType(), True),
        StructField("stock_code", StringType(), True),
        StructField("description", StringType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("invoice_date", TimestampType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("customer_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("is_cancellation", BooleanType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Helper: build a minimal valid raw row
# ---------------------------------------------------------------------------

def _dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def make_raw_rows(spark: SparkSession, rows: List[dict]) -> DataFrame:
    """
    Build a raw DataFrame from a list of dicts.
    Missing keys default to None; types are coerced via RAW_SCHEMA.
    """
    defaults = {
        "InvoiceNo": "536365",
        "StockCode": "85123",
        "Description": "Product 85123",
        "Quantity": 10.0,
        "InvoiceDate": _dt("2011-06-01 10:00:00"),
        "UnitPrice": 2.50,
        "CustomerID": "17850.0",
        "Country": "United Kingdom",
        "Revenue": 25.0,
    }
    completed = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [Row(**r) for r in completed], schema=RAW_SCHEMA
    )


# ---------------------------------------------------------------------------
# Fixtures: ready-made small DataFrames used across multiple test modules
# ---------------------------------------------------------------------------


@pytest.fixture()
def raw_valid_df(spark: SparkSession) -> DataFrame:
    """A single fully-valid raw row."""
    return make_raw_rows(spark, [{}])


@pytest.fixture()
def raw_with_nulls_df(spark: SparkSession) -> DataFrame:
    """Rows covering every null / edge case the cleaner must handle."""
    return make_raw_rows(
        spark,
        [
            {},                                                  # valid baseline
            {"InvoiceNo": None},                                 # null InvoiceNo  → drop
            {"InvoiceNo": ""},                                   # empty InvoiceNo → drop
            {"Quantity": None},                                  # null Quantity   → drop
            {"InvoiceDate": None},                               # null InvoiceDate→ drop
            {"UnitPrice": None},                                 # null UnitPrice  → drop
            {"StockCode": None},                                 # null StockCode  → UNKNOWN
            {"StockCode": ""},                                   # empty StockCode → UNKNOWN
            {"Country": None},                                   # null Country    → Unknown
            {"CustomerID": None},                                # null CustomerID → ANONYMOUS
            {"InvoiceNo": "C536381", "Revenue": 2539.9},         # cancellation flag
            {"StockCode": "82804.0"},                            # float StockCode → strip .0
            {"CustomerID": "16016.0"},                           # float CustomerID
        ],
    )
