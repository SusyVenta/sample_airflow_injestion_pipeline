"""
clean_and_ingest.py
-------------------
PySpark script that:
  1. Loads the raw Online Retail CSV dataset using an explicit schema (RETAIL_CSV_SCHEMA)
     by default, eliminating type ambiguity and the two-pass cost of inferSchema.
  2. Applies a cleaning pipeline (missing values, duplicates, type coercion, anomalies).
  3. Anonymises CustomerID (PII) via SHA-256 hashing.
  4. Recalculates Revenue to fix floating-point inconsistencies.
  5. Appends the cleaned data to a PostgreSQL table via JDBC, tagging every row with
     a loaded_at timestamp so pipeline runs can be distinguished.

Run standalone:
    spark-submit \\
        --packages org.postgresql:postgresql:42.7.1 \\
        spark/jobs/clean_and_ingest.py

Environment variables (all have sensible defaults for the docker-compose stack):
    CSV_PATH          – path to the raw CSV (default: /opt/airflow/data/retails.csv)
    POSTGRES_HOST     – PostgreSQL hostname          (default: postgres)
    POSTGRES_PORT     – PostgreSQL port              (default: 5432)
    POSTGRES_DB       – database name                (default: retail)
    POSTGRES_USER     – database user                (default: retail)
    POSTGRES_PASSWORD – database password            (default: retail)
"""

from __future__ import annotations

import os
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CSV_PATH: str = os.getenv("CSV_PATH", "/opt/airflow/data/retails.csv")

POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "retail")
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "retail")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "retail")

POSTGRES_URL: str = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
POSTGRES_PROPS: Dict[str, str] = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

TARGET_TABLE: str = "retail_transactions"

# ---------------------------------------------------------------------------
# Enforced CSV schema
# ---------------------------------------------------------------------------
# Preferred over inferSchema=True because:
#   - Single-pass read: inferSchema scans the file twice.
#   - CustomerID is loaded as StringType, eliminating the '.0' artifact that
#     inferSchema introduces when it infers the column as DoubleType.
#   - InvoiceDate is loaded as StringType; cast_types() converts it to
#     TimestampType downstream, handling the non-ISO date format in the CSV.
#   - Deterministic: schema never changes between runs regardless of data sample.

RETAIL_CSV_SCHEMA: StructType = StructType(
    [
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", DoubleType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Revenue", DoubleType(), True),
    ]
)

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------


def create_spark_session(app_name: str = "RetailDataCleaning") -> SparkSession:
    """Return a SparkSession pre-configured for the retail pipeline."""
    return (
        SparkSession.builder.appName(app_name)
        # JDBC driver downloaded at runtime via Maven coordinates
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def load_raw_data(
    spark: SparkSession,
    path: str,
    enforce_schema: bool = True,
) -> DataFrame:
    """Load the raw CSV.

    enforce_schema=True (default): applies RETAIL_CSV_SCHEMA explicitly.
    enforce_schema=False: falls back to Spark's automatic schema inference
    (two-pass read; types may vary across runs and data samples).
    """
    if enforce_schema:
        return spark.read.csv(path, header=True, schema=RETAIL_CSV_SCHEMA)
    return spark.read.csv(path, header=True, inferSchema=True)


# ---------------------------------------------------------------------------
# Individual cleaning transformations
# ---------------------------------------------------------------------------


def drop_invalid_rows(df: DataFrame) -> DataFrame:
    """
    Drop rows where fields that are essential for analysis are absent:
      - InvoiceNo  : needed for transaction identification
      - Quantity   : needed for revenue calculation
      - InvoiceDate: needed for every time-based analysis
      - UnitPrice  : needed for revenue calculation

    An empty string InvoiceNo (observed in source data) is treated as null.
    """
    return df.filter(
        F.col("InvoiceNo").isNotNull()
        & (F.trim(F.col("InvoiceNo").cast(StringType())) != "")
        & F.col("Quantity").isNotNull()
        & F.col("InvoiceDate").isNotNull()
        & F.col("UnitPrice").isNotNull()
    )


def flag_cancellations(df: DataFrame) -> DataFrame:
    """
    Add boolean column `is_cancellation`.
    Per the dataset spec, an InvoiceNo starting with 'C' marks a cancellation.
    """
    return df.withColumn(
        "is_cancellation",
        F.col("InvoiceNo").cast(StringType()).startswith("C"),
    )


def clean_stock_code(df: DataFrame) -> DataFrame:
    """
    Normalise StockCode:
      - Strip the trailing '.0' that CSV inference adds to numeric codes
        (e.g. '82804.0'  ->  '82804').
      - Replace null / empty values with the sentinel 'UNKNOWN'.
    """
    normalised = F.regexp_replace(
        F.col("StockCode").cast(StringType()), r"\.0$", ""
    )
    return df.withColumn(
        "StockCode",
        F.when(
            F.col("StockCode").isNull()
            | (F.trim(F.col("StockCode").cast(StringType())) == ""),
            F.lit("UNKNOWN"),
        ).otherwise(normalised),
    )


def fill_missing_country(df: DataFrame) -> DataFrame:
    """Replace null Country with the sentinel 'Unknown'."""
    return df.withColumn(
        "Country",
        F.coalesce(F.col("Country"), F.lit("Unknown")),
    )


def anonymise_customer_id(df: DataFrame) -> DataFrame:
    """
    Anonymise CustomerID (PII) using SHA-256.

    Rationale: CustomerID is classified as PII. We replace the raw integer
    identifier with a deterministic, one-way hash so that downstream analytics
    can still group by customer without exposing the original value.

    Null CustomerIDs (guest / anonymous purchases) are mapped to the fixed
    string 'ANONYMOUS' rather than hashed to preserve the semantic meaning.

    CustomerID is stored as a float by CSV inference (e.g. '16016.0');
    the trailing '.0' is stripped before hashing so that the same customer
    always produces the same hash regardless of ingestion format.
    """
    normalised_id = F.regexp_replace(
        F.col("CustomerID").cast(StringType()), r"\.0$", ""
    )
    return df.withColumn(
        "CustomerID",
        F.when(
            F.col("CustomerID").isNotNull(),
            F.sha2(normalised_id, 256),
        ).otherwise(F.lit("ANONYMOUS")),
    )


def recalculate_revenue(df: DataFrame) -> DataFrame:
    """
    Recompute Revenue = round(Quantity * UnitPrice, 2).

    The source CSV contains a pre-calculated Revenue column that suffers from
    floating-point drift (e.g. 1697.4499999999998 instead of 1697.45).
    Recomputing ensures consistency and correctness.
    """
    return df.withColumn(
        "Revenue",
        F.round(F.col("Quantity") * F.col("UnitPrice"), 2),
    )


def cast_types(df: DataFrame) -> DataFrame:
    """Enforce the canonical types for every column."""
    return (
        df.withColumn("InvoiceNo", F.col("InvoiceNo").cast(StringType()))
        .withColumn("StockCode", F.col("StockCode").cast(StringType()))
        .withColumn("Description", F.col("Description").cast(StringType()))
        .withColumn("Quantity", F.col("Quantity").cast(DoubleType()))
        .withColumn("InvoiceDate", F.col("InvoiceDate").cast(TimestampType()))
        .withColumn("UnitPrice", F.col("UnitPrice").cast(DoubleType()))
        .withColumn("Country", F.col("Country").cast(StringType()))
        .withColumn("Revenue", F.col("Revenue").cast(DoubleType()))
    )


def rename_to_snake_case(df: DataFrame) -> DataFrame:
    """Rename CamelCase source columns to snake_case for PostgreSQL compatibility."""
    mapping = {
        "InvoiceNo": "invoice_no",
        "StockCode": "stock_code",
        "Description": "description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "UnitPrice": "unit_price",
        "CustomerID": "customer_id",
        "Country": "country",
        "Revenue": "revenue",
    }
    for old, new in mapping.items():
        df = df.withColumnRenamed(old, new)
    return df


# ---------------------------------------------------------------------------
# Cleaning pipeline
# ---------------------------------------------------------------------------


def clean_data(df: DataFrame) -> DataFrame:
    """
    Execute the full cleaning pipeline in order:

      1. Remove exact duplicate rows.
      2. Drop rows with missing critical fields.
      3. Flag cancellation transactions.
      4. Normalise / fill StockCode.
      5. Fill missing Country.
      6. Anonymise CustomerID (PII).
      7. Recalculate Revenue.
      8. Enforce correct column types.
      9. Rename columns to snake_case.

    Returns a cleaned DataFrame ready to be written to PostgreSQL.
    """
    df = df.dropDuplicates()
    df = drop_invalid_rows(df)
    df = flag_cancellations(df)
    df = clean_stock_code(df)
    df = fill_missing_country(df)
    df = anonymise_customer_id(df)
    df = recalculate_revenue(df)
    df = cast_types(df)
    df = rename_to_snake_case(df)
    return df


# ---------------------------------------------------------------------------
# PostgreSQL write
# ---------------------------------------------------------------------------


def load_to_postgres(df: DataFrame, table: str) -> None:
    """Append a DataFrame to PostgreSQL via JDBC with a loaded_at timestamp.

    Each pipeline run appends its rows tagged with the current UTC timestamp so
    that runs can be distinguished. Query the latest snapshot with:
        WHERE loaded_at = (SELECT MAX(loaded_at) FROM <table>)
    """
    df.withColumn("loaded_at", F.current_timestamp()).write.jdbc(
        url=POSTGRES_URL,
        table=table,
        mode="append",
        properties=POSTGRES_PROPS,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[clean_and_ingest] Reading CSV: {CSV_PATH}")
    raw_df = load_raw_data(spark, CSV_PATH)
    raw_count = raw_df.count()
    print(f"[clean_and_ingest] Raw row count : {raw_count:,}")

    cleaned_df = clean_data(raw_df)
    cleaned_count = cleaned_df.count()
    dropped = raw_count - cleaned_count
    print(f"[clean_and_ingest] Cleaned row count : {cleaned_count:,}")
    print(f"[clean_and_ingest] Rows dropped (invalid/duplicate): {dropped:,}")

    print(f"[clean_and_ingest] Writing to PostgreSQL table '{TARGET_TABLE}' ...")
    load_to_postgres(cleaned_df, TARGET_TABLE)
    print("[clean_and_ingest] Done.")

    spark.stop()


if __name__ == "__main__":
    main()
