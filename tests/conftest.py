"""
conftest.py
-----------
Shared pytest fixtures for the retail pipeline test suite.

A single SparkSession in local mode is created once per test session to keep
test execution fast.  Individual tests receive DataFrames constructed from
plain Python lists, so no external services (PostgreSQL, HDFS) are needed.
"""

from __future__ import annotations

from datetime import datetime
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
