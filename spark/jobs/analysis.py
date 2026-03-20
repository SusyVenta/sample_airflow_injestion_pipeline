"""
analysis.py
-----------
PySpark script that reads the cleaned retail data from PostgreSQL and answers
three analytical questions:

  1. What is the total revenue generated in the dataset?
  2. Which are the top 10 most popular products based on the quantity sold?
  3. What is the monthly revenue trend?  Are there noticeable patterns or
     anomalies?

Results are printed to stdout (visible in Airflow task logs) and also
persisted back to PostgreSQL as analysis tables for downstream SQL access.

Run standalone:
    spark-submit \\
        --packages org.postgresql:postgresql:42.7.1 \\
        spark/jobs/analysis.py

Environment variables (defaults match the docker-compose stack):
    POSTGRES_HOST / POSTGRES_PORT / POSTGRES_DB / POSTGRES_USER / POSTGRES_PASSWORD
"""

from __future__ import annotations

import os
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

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

SOURCE_TABLE: str = "retail_transactions"

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------


def create_spark_session(app_name: str = "RetailAnalysis") -> SparkSession:
    """Return a SparkSession configured for the retail analytics job."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_from_postgres(spark: SparkSession, table: str) -> DataFrame:
    """Load a table from PostgreSQL into a Spark DataFrame."""
    return spark.read.jdbc(
        url=POSTGRES_URL,
        table=table,
        properties=POSTGRES_PROPS,
    )


def get_valid_sales(df: DataFrame) -> DataFrame:
    """
    Filter to non-cancelled transactions with strictly positive revenue.

    Cancellations (is_cancellation = True) and rows with non-positive revenue
    (e.g. negative unit-price adjustments) are excluded so that aggregations
    reflect genuine sales, not returns or accounting corrections.
    """
    return df.filter(
        (~F.col("is_cancellation"))
        & (F.col("revenue") > 0)
        & F.col("invoice_date").isNotNull()
    )


# ---------------------------------------------------------------------------
# Analysis 1 – Total revenue
# ---------------------------------------------------------------------------


def calculate_total_revenue(df: DataFrame) -> float:
    """
    Return the total net revenue: gross sales minus cancellations/returns.

    Revenue is the sum of (Quantity * UnitPrice) rounded to two decimal places.
    Cancellation rows carry negative revenue and must be included so that
    reimbursed orders do not inflate the total.  Pass the full (unfiltered)
    DataFrame to get net revenue; passing only valid sales gives gross revenue.
    """
    result = (
        df.agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
        .collect()
    )
    return float(result[0]["total_revenue"] or 0.0)


# ---------------------------------------------------------------------------
# Analysis 2 – Top 10 products by quantity sold
# ---------------------------------------------------------------------------


def get_top_10_products(df: DataFrame) -> DataFrame:
    """
    Return the top 10 products by total quantity sold.

    Output columns: stock_code, quantity_sold.
    Revenue is computed internally only as a tiebreaker when two products
    share the same quantity; it is not included in the output.
    """
    return (
        df.filter(F.col("stock_code") != "UNKNOWN")
        .groupBy("stock_code")
        .agg(
            F.sum("quantity").alias("quantity_sold"),
            F.sum("revenue").alias("_revenue_tiebreaker"),
        )
        .orderBy(F.desc("quantity_sold"), F.desc("_revenue_tiebreaker"))
        .limit(10)
        .drop("_revenue_tiebreaker")
    )


# ---------------------------------------------------------------------------
# Analysis 3 – Monthly revenue trend
# ---------------------------------------------------------------------------


def get_monthly_revenue_trend(df: DataFrame) -> DataFrame:
    """
    Aggregate net revenue by calendar month and compute month-over-month (MoM)
    growth rate.

    All transactions are included — cancellations carry negative revenue and
    are summed in so the result is net monthly revenue (gross sales minus
    returns), not gross-only.  The only pre-filter applied is removing rows
    with a null invoice_date, which cannot be assigned to any month.

    Columns returned:
      year_month       – 'YYYY-MM' label
      monthly_revenue  – net sum of revenue for the month (sales minus returns)
      num_transactions – count of distinct invoices (including cancellations)
      num_customers    – count of distinct (anonymised) customer IDs
      mom_growth_pct   – percentage change vs the previous month (null for first month)
    """
    monthly = (
        df.filter(F.col("invoice_date").isNotNull())
        .withColumn("year_month", F.date_format("invoice_date", "yyyy-MM"))
        .groupBy("year_month")
        .agg(
            F.round(F.sum("revenue"), 2).alias("monthly_revenue"),
            F.countDistinct("invoice_no").alias("num_transactions"),
            F.countDistinct("customer_id").alias("num_customers"),
        )
        .orderBy("year_month")
    )

    # Month-over-month growth using a lag window ordered by the month label.
    # partitionBy(lit(0)) puts all rows in one explicit partition, suppressing
    # the "No Partition Defined for Window operation" warning that Spark emits
    # when orderBy is used without partitionBy.
    w = Window.partitionBy(F.lit(0)).orderBy("year_month")
    monthly = (
        monthly
        .withColumn("prev_revenue", F.lag("monthly_revenue").over(w))
        .withColumn(
            "mom_growth_pct",
            F.round(
                (F.col("monthly_revenue") - F.col("prev_revenue"))
                / F.col("prev_revenue") * 100,
                2,
            ),
        )
        .drop("prev_revenue")
        # Re-apply orderBy: window functions do not preserve the earlier sort
        .orderBy("year_month")
    )
    return monthly


def print_monthly_insights(monthly_df: DataFrame) -> None:
    """
    Print a human-readable summary with:
      - Per-month revenue, MoM growth, transaction count
      - Peak and trough months
      - Anomaly flags (months more than 2 standard deviations from the mean)
      - A note on expected seasonal patterns for online retail
    """
    rows = monthly_df.orderBy("year_month").collect()
    revenues: List[float] = [
        float(r["monthly_revenue"]) for r in rows if r["monthly_revenue"] is not None
    ]

    if not revenues:
        print("[Analysis 3] No monthly data found.")
        return

    avg_rev = sum(revenues) / len(revenues)
    variance = sum((r - avg_rev) ** 2 for r in revenues) / len(revenues)
    std_rev = variance ** 0.5

    print("\n" + "=" * 72)
    print("  Monthly Revenue Trend")
    print("=" * 72)
    print(
        f"  {'Month':<10} {'Revenue (GBP)':>15} {'MoM Growth':>12}"
        f"  {'Invoices':>8}  {'Customers':>9}"
    )
    print("-" * 72)

    for row in rows:
        rev = row["monthly_revenue"] or 0.0
        growth = row["mom_growth_pct"]
        growth_str = f"{growth:+.1f}%" if growth is not None else "    --"
        anomaly = " [ANOMALY >2σ]" if abs(rev - avg_rev) > 2 * std_rev else ""
        print(
            f"  {row['year_month']:<10} {rev:>15,.2f} {growth_str:>12}"
            f"  {row['num_transactions']:>8,}  {row['num_customers']:>9,}"
            f"{anomaly}"
        )

    print("=" * 72)

    if rows:
        peak = max(rows, key=lambda r: r["monthly_revenue"] or 0.0)
        trough = min(rows, key=lambda r: r["monthly_revenue"] or float("inf"))
        print(
            f"\n  Peak month  : {peak['year_month']}"
            f"  (GBP {peak['monthly_revenue']:,.2f})"
        )
        print(
            f"  Trough month: {trough['year_month']}"
            f"  (GBP {trough['monthly_revenue']:,.2f})"
        )

    print(
        "\n  Interpretation notes:"
        "\n  - Online retail characteristically peaks in Q4 (Oct–Dec) driven by"
        "\n    holiday shopping; a spike in November/December is expected."
        "\n  - Any month flagged [ANOMALY] deviates >2 std deviations from the"
        "\n    dataset mean and warrants further investigation (e.g. missing data,"
        "\n    promotional campaigns, or system outages causing under-reporting)."
        "\n  - Months with unusually low transaction counts relative to revenue"
        "\n    may indicate bulk/wholesale orders rather than retail sales."
        "\n"
    )


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------


def save_to_postgres(df: DataFrame, table: str) -> None:
    """Persist a result DataFrame to PostgreSQL, overwriting any prior run."""
    df.write.jdbc(
        url=POSTGRES_URL,
        table=table,
        mode="overwrite",
        properties=POSTGRES_PROPS,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("[analysis] Loading cleaned data from PostgreSQL ...")
    df = load_from_postgres(spark, SOURCE_TABLE)
    valid_df = get_valid_sales(df)

    # ------------------------------------------------------------------
    # 1. Total net revenue (cancellations subtracted)
    # ------------------------------------------------------------------
    total_revenue = calculate_total_revenue(df)
    print(f"\n[Analysis 1] Total Net Revenue: GBP {total_revenue:,.2f}")

    # ------------------------------------------------------------------
    # 2. Top 10 products by quantity sold
    # ------------------------------------------------------------------
    print("\n[Analysis 2] Top 10 Most Popular Products by Quantity Sold:")
    top10 = get_top_10_products(valid_df)
    top10.show(truncate=False)
    save_to_postgres(top10, "analysis_top10_products")

    # ------------------------------------------------------------------
    # 3. Monthly revenue trend (net: includes cancellations)
    # ------------------------------------------------------------------
    print("\n[Analysis 3] Monthly Revenue Trend:")
    monthly = get_monthly_revenue_trend(df)
    print_monthly_insights(monthly)
    save_to_postgres(monthly, "analysis_monthly_revenue")

    print("[analysis] Done.")
    spark.stop()


if __name__ == "__main__":
    main()
