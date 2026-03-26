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
    """Load the latest batch from a PostgreSQL table into a Spark DataFrame.

    The table uses Type 2 append: every pipeline run adds rows tagged with
    loaded_at.  We scope to MAX(loaded_at) so aggregations only see one
    consistent snapshot and are not inflated by rows from prior runs.
    """
    latest_batch = (
        f"(SELECT * FROM {table}"
        f" WHERE loaded_at = (SELECT MAX(loaded_at) FROM {table})) AS latest"
    )
    return spark.read.jdbc(
        url=POSTGRES_URL,
        table=latest_batch,
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

    # Both MoM and YoY windows use partitionBy(lit(0)) to place all rows in
    # one explicit global partition ordered by month label.  This makes the
    # intent clear to Spark and suppresses the "No Partition Defined for Window
    # operation" warning that would otherwise be emitted.
    # All windows use partitionBy(lit(0)) to place all rows in one explicit
    # global partition ordered by month label.  This makes the intent clear to
    # Spark and suppresses the "No Partition Defined for Window operation"
    # warning that would otherwise be emitted.
    w = Window.partitionBy(F.lit(0)).orderBy("year_month")
    w3 = w.rowsBetween(-2, 0)   # 3-month rolling window (current + 2 prior)
    w6 = w.rowsBetween(-5, 0)   # 6-month rolling window (current + 5 prior)

    monthly = (
        monthly
        # --- Month-over-month growth ---
        .withColumn("prev_revenue", F.lag("monthly_revenue", 1).over(w))
        .withColumn(
            "mom_growth_pct",
            F.round(
                (F.col("monthly_revenue") - F.col("prev_revenue"))
                / F.col("prev_revenue") * 100,
                2,
            ),
        )
        .drop("prev_revenue")
        # --- Year-over-year: same month 12 periods back ---
        # Null for months where fewer than 12 prior months exist in the dataset.
        .withColumn("prev_year_revenue", F.lag("monthly_revenue", 12).over(w))
        .withColumn(
            "yoy_growth_pct",
            F.round(
                (F.col("monthly_revenue") - F.col("prev_year_revenue"))
                / F.col("prev_year_revenue") * 100,
                2,
            ),
        )
        .drop("prev_year_revenue")
        # --- Rolling 3-month average (reference baseline displayed in output) ---
        .withColumn("rolling_3m_avg", F.round(F.avg("monthly_revenue").over(w3), 2))
        # --- Anomaly indicator 1: revenue level z-score (6-month window) ---
        # z-score = how many standard deviations a value is from the mean.
        # A 6-month window gives a more stable baseline than 3 months.
        # With 3 months, a single outlier dominates the std and shrinks its
        # own sigma-distance toward zero (self-normalisation).  With 6 months
        # the 5 surrounding months anchor the mean and std more reliably.
        # Threshold: ±1.645σ ≈ p90 two-tailed (flags ~10% of observations).
        .withColumn("_6m_rev_avg", F.avg("monthly_revenue").over(w6))
        .withColumn("_6m_rev_std", F.stddev("monthly_revenue").over(w6))
        .withColumn(
            "rev_sigma_dist",
            F.round(
                (F.col("monthly_revenue") - F.col("_6m_rev_avg"))
                / F.col("_6m_rev_std"),
                2,
            ),
        )
        .drop("_6m_rev_avg", "_6m_rev_std")
        # --- Anomaly indicator 2: MoM growth z-score (6-month window) ---
        # Measures whether the *rate of change* this month is unusual relative
        # to recent momentum.  This catches sudden structural breaks (e.g. a
        # +15 000 % MoM jump or a -70 % collapse) that the revenue-level
        # indicator may miss when the 6-month std is itself large.
        .withColumn("_6m_mom_avg", F.avg("mom_growth_pct").over(w6))
        .withColumn("_6m_mom_std", F.stddev("mom_growth_pct").over(w6))
        .withColumn(
            "mom_sigma_dist",
            F.round(
                (F.col("mom_growth_pct") - F.col("_6m_mom_avg"))
                / F.col("_6m_mom_std"),
                2,
            ),
        )
        .drop("_6m_mom_avg", "_6m_mom_std")
        # Re-apply orderBy: window functions do not preserve the earlier sort
        .orderBy("year_month")
    )
    return monthly


_ANOMALY_THRESHOLD: float = 1.645   # ±1.645σ ≈ p90 two-tailed


def print_monthly_insights(monthly_df: DataFrame) -> None:
    """
    Print a human-readable summary with two complementary anomaly indicators:

      rev-σ  – 6-month rolling z-score on revenue level.  Catches months
               where the absolute revenue is far from recent norms.
      mom-σ  – 6-month rolling z-score on MoM growth rate.  Catches sudden
               structural breaks (large jumps or collapses) that the revenue-
               level indicator can miss when the rolling std is itself large.

    Both use ±1.645σ as the flag threshold (≈ p90 two-tailed).  A month
    flagged by both indicators is a stronger signal than one flagged by either
    alone.  Flag column shows REV / MOM / BOTH.
    """
    rows = monthly_df.orderBy("year_month").collect()

    if not rows:
        print("[Analysis 3] No monthly data found.")
        return

    w = 130
    print("\n" + "=" * w)
    print("  Monthly Net Revenue Trend  (net: gross sales minus cancellations/returns)")
    print(f"  Anomaly threshold: ±{_ANOMALY_THRESHOLD}σ (p90 two-tailed) on 6-month rolling windows")
    print("=" * w)
    print(
        f"  {'Month':<10} {'Revenue (GBP)':>15} {'3m Avg (GBP)':>14}"
        f" {'MoM':>9} {'YoY':>9}  {'rev-σ(6m)':>10}  {'mom-σ(6m)':>10}  {'Flag':<6}"
        f"  {'Invoices':>8}  {'Customers':>9}"
    )
    print("-" * w)

    for row in rows:
        rev = row["monthly_revenue"] or 0.0
        avg3 = row["rolling_3m_avg"]
        mom = row["mom_growth_pct"]
        yoy = row["yoy_growth_pct"]
        rev_s = row["rev_sigma_dist"]
        mom_s = row["mom_sigma_dist"]

        avg3_str = f"{avg3:,.2f}" if avg3 is not None else "            --"
        mom_str = f"{mom:+.1f}%" if mom is not None else "     --"
        yoy_str = f"{yoy:+.1f}%" if yoy is not None else "     --"
        rev_s_str = f"{rev_s:+.2f}σ" if rev_s is not None else "        --"
        mom_s_str = f"{mom_s:+.2f}σ" if mom_s is not None else "        --"

        rev_flagged = rev_s is not None and abs(rev_s) > _ANOMALY_THRESHOLD
        mom_flagged = mom_s is not None and abs(mom_s) > _ANOMALY_THRESHOLD
        if rev_flagged and mom_flagged:
            flag = "BOTH"
        elif rev_flagged:
            flag = "REV"
        elif mom_flagged:
            flag = "MOM"
        else:
            flag = ""

        print(
            f"  {row['year_month']:<10} {rev:>15,.2f} {avg3_str:>14}"
            f" {mom_str:>9} {yoy_str:>9}  {rev_s_str:>10}  {mom_s_str:>10}  {flag:<6}"
            f"  {row['num_transactions']:>8,}  {row['num_customers']:>9,}"
        )

    print("=" * w)
    print(
        "  rev-σ(6m): (revenue − 6m rolling avg) / 6m rolling std\n"
        "  mom-σ(6m): (MoM% − 6m rolling avg MoM%) / 6m rolling std MoM%\n"
        "  Flag: REV = revenue level outlier | MOM = rate-of-change outlier | BOTH = both"
    )

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
        "\n  - Revenue is NET (gross sales minus returns). Months with unusually"
        "\n    high return volumes may show lower or even negative net revenue."
        "\n  - Two complementary indicators catch different failure modes:"
        "\n    rev-σ detects sustained level anomalies; mom-σ detects sudden"
        "\n    structural breaks regardless of absolute revenue level."
        "\n  - A 6-month window prevents self-normalisation: with a 3-month window"
        "\n    a single outlier dominates the std and shrinks its own σ-distance"
        "\n    toward zero. Six months gives 5 anchor points to stabilise the baseline."
        "\n  - σ values are NULL when fewer data points exist than the window"
        "\n    needs to compute a meaningful standard deviation."
        "\n  - Online retail characteristically peaks in Q4 (Oct–Dec). A YoY dip"
        "\n    in a Q4 month may reflect a partial month at the dataset boundary"
        "\n    rather than a real decline (e.g. data ending mid-December)."
        "\n  - YoY '--' means no data exists for the same month one year earlier."
        "\n"
    )


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------


def save_to_postgres(df: DataFrame, table: str) -> None:
    """Append a result DataFrame to PostgreSQL with a loaded_at timestamp.

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
