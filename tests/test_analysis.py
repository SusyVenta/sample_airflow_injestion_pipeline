"""
test_analysis.py
----------------
Unit tests for the PySpark analysis functions in analysis.py.

All tests use a local SparkSession and in-memory DataFrames.
No PostgreSQL connection is required.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Import functions under test
from spark.jobs.analysis import (
    calculate_total_revenue,
    get_monthly_revenue_trend,
    get_top_10_products,
    get_valid_sales,
)


# ---------------------------------------------------------------------------
# Schema for the cleaned retail_transactions table
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
# Helpers
# ---------------------------------------------------------------------------


def _dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def _make_cleaned(spark: SparkSession, rows: list[dict]) -> DataFrame:
    defaults = {
        "invoice_no": "536365",
        "stock_code": "85123",
        "description": "Product A",
        "quantity": 10.0,
        "invoice_date": _dt("2011-06-01 10:00:00"),
        "unit_price": 2.50,
        "customer_id": "abcdef1234",
        "country": "United Kingdom",
        "revenue": 25.0,
        "is_cancellation": False,
    }
    completed = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [Row(**r) for r in completed], schema=CLEANED_SCHEMA
    )


# ---------------------------------------------------------------------------
# get_valid_sales
# ---------------------------------------------------------------------------


class TestGetValidSales:
    def test_keeps_normal_sale(self, spark: SparkSession) -> None:
        df = _make_cleaned(spark, [{}])
        assert get_valid_sales(df).count() == 1

    def test_excludes_cancellations(self, spark: SparkSession) -> None:
        df = _make_cleaned(spark, [{"is_cancellation": True}])
        assert get_valid_sales(df).count() == 0

    def test_excludes_non_positive_revenue(self, spark: SparkSession) -> None:
        df = _make_cleaned(spark, [{"revenue": 0.0}, {"revenue": -10.0}])
        assert get_valid_sales(df).count() == 0

    def test_excludes_null_invoice_date(self, spark: SparkSession) -> None:
        df = _make_cleaned(spark, [{"invoice_date": None}])
        assert get_valid_sales(df).count() == 0

    def test_mixed_rows(self, spark: SparkSession) -> None:
        df = _make_cleaned(
            spark,
            [
                {},                                  # valid
                {"is_cancellation": True},           # excluded
                {"revenue": -5.0},                   # excluded
                {"invoice_date": None},              # excluded
            ],
        )
        assert get_valid_sales(df).count() == 1


# ---------------------------------------------------------------------------
# calculate_total_revenue
# ---------------------------------------------------------------------------


class TestCalculateTotalRevenue:
    def test_sums_revenue(self, spark: SparkSession) -> None:
        df = _make_cleaned(
            spark, [{"revenue": 100.0}, {"revenue": 200.0}, {"revenue": 50.0}]
        )
        assert calculate_total_revenue(df) == 350.0

    def test_single_row(self, spark: SparkSession) -> None:
        df = _make_cleaned(spark, [{"revenue": 999.99}])
        assert calculate_total_revenue(df) == 999.99

    def test_empty_dataframe_returns_zero(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([], schema=CLEANED_SCHEMA)
        assert calculate_total_revenue(df) == 0.0

    def test_result_rounded_to_two_decimals(self, spark: SparkSession) -> None:
        # 0.1 + 0.2 in floating point = 0.30000000000000004;
        # the function should round to 0.30
        df = _make_cleaned(
            spark, [{"revenue": 0.1}, {"revenue": 0.2}]
        )
        assert calculate_total_revenue(df) == 0.30


# ---------------------------------------------------------------------------
# get_top_10_products
# ---------------------------------------------------------------------------


class TestGetTop10Products:
    def test_returns_at_most_10_rows(self, spark: SparkSession) -> None:
        rows = [{"stock_code": str(i), "description": f"Prod {i}", "quantity": float(i)}
                for i in range(1, 20)]
        df = _make_cleaned(spark, rows)
        result = get_top_10_products(df)
        assert result.count() == 10

    def test_ordered_by_quantity_descending(self, spark: SparkSession) -> None:
        df = _make_cleaned(
            spark,
            [
                {"stock_code": "A", "description": "Low", "quantity": 5.0, "revenue": 10.0},
                {"stock_code": "B", "description": "High", "quantity": 100.0, "revenue": 200.0},
                {"stock_code": "C", "description": "Mid", "quantity": 50.0, "revenue": 100.0},
            ],
        )
        result = get_top_10_products(df)
        codes = [r["stock_code"] for r in result.collect()]
        assert codes == ["B", "C", "A"]

    def test_ties_broken_by_revenue(self, spark: SparkSession) -> None:
        """When two products have the same quantity, higher revenue wins."""
        df = _make_cleaned(
            spark,
            [
                {"stock_code": "A", "description": "Cheap", "quantity": 10.0, "revenue": 20.0},
                {"stock_code": "B", "description": "Pricey", "quantity": 10.0, "revenue": 50.0},
            ],
        )
        result = get_top_10_products(df)
        codes = [r["stock_code"] for r in result.collect()]
        assert codes[0] == "B"  # higher revenue comes first

    def test_aggregates_across_multiple_transactions(self, spark: SparkSession) -> None:
        """The same product appearing in multiple rows must be summed."""
        df = _make_cleaned(
            spark,
            [
                {"stock_code": "X", "description": "Widget", "quantity": 10.0, "revenue": 25.0},
                {"stock_code": "X", "description": "Widget", "quantity": 20.0, "revenue": 50.0},
                {"stock_code": "Y", "description": "Gadget", "quantity": 15.0, "revenue": 30.0},
            ],
        )
        result = get_top_10_products(df)
        rows = {r["stock_code"]: r for r in result.collect()}
        assert rows["X"]["quantity_sold"] == 30.0
        assert rows["Y"]["quantity_sold"] == 15.0
        # X has more quantity, so it should be first
        codes = [r["stock_code"] for r in result.collect()]
        assert codes[0] == "X"

    def test_output_columns(self, spark: SparkSession) -> None:
        """Result must contain exactly stock_code and quantity_sold."""
        df = _make_cleaned(spark, [{}])
        result = get_top_10_products(df)
        assert result.columns == ["stock_code", "quantity_sold"]

    def test_unknown_stock_code_excluded(self, spark: SparkSession) -> None:
        df = _make_cleaned(spark, [{"stock_code": "UNKNOWN", "quantity": 999.0}])
        assert get_top_10_products(df).count() == 0


# ---------------------------------------------------------------------------
# get_monthly_revenue_trend
# ---------------------------------------------------------------------------


class TestGetMonthlyRevenueTrend:
    def test_returns_correct_months(self, spark: SparkSession) -> None:
        df = _make_cleaned(
            spark,
            [
                {"invoice_date": _dt("2011-01-15 10:00:00"), "revenue": 100.0},
                {"invoice_date": _dt("2011-02-10 10:00:00"), "revenue": 200.0},
                {"invoice_date": _dt("2011-03-20 10:00:00"), "revenue": 150.0},
            ],
        )
        result = get_monthly_revenue_trend(df)
        months = [r["year_month"] for r in result.orderBy("year_month").collect()]
        assert months == ["2011-01", "2011-02", "2011-03"]

    def test_aggregates_same_month_correctly(self, spark: SparkSession) -> None:
        df = _make_cleaned(
            spark,
            [
                {"invoice_date": _dt("2011-06-01 10:00:00"), "revenue": 100.0},
                {"invoice_date": _dt("2011-06-15 10:00:00"), "revenue": 200.0},
            ],
        )
        result = get_monthly_revenue_trend(df)
        assert result.count() == 1
        row = result.collect()[0]
        assert row["monthly_revenue"] == 300.0

    def test_first_month_has_null_mom_growth(self, spark: SparkSession) -> None:
        df = _make_cleaned(
            spark,
            [
                {"invoice_date": _dt("2011-01-01 10:00:00"), "revenue": 100.0},
                {"invoice_date": _dt("2011-02-01 10:00:00"), "revenue": 200.0},
            ],
        )
        result = get_monthly_revenue_trend(df).orderBy("year_month")
        rows = result.collect()
        assert rows[0]["mom_growth_pct"] is None   # first month: no prior month
        assert rows[1]["mom_growth_pct"] == 100.0  # 100 → 200 = +100%

    def test_mom_growth_calculated_correctly(self, spark: SparkSession) -> None:
        # 100 → 150: growth = (150-100)/100 * 100 = 50%
        df = _make_cleaned(
            spark,
            [
                {"invoice_date": _dt("2011-03-01 10:00:00"), "revenue": 100.0},
                {"invoice_date": _dt("2011-04-01 10:00:00"), "revenue": 150.0},
            ],
        )
        result = get_monthly_revenue_trend(df).orderBy("year_month")
        rows = result.collect()
        assert rows[1]["mom_growth_pct"] == 50.0

    def test_negative_mom_growth(self, spark: SparkSession) -> None:
        # 200 → 100: growth = -50%
        df = _make_cleaned(
            spark,
            [
                {"invoice_date": _dt("2011-05-01 10:00:00"), "revenue": 200.0},
                {"invoice_date": _dt("2011-06-01 10:00:00"), "revenue": 100.0},
            ],
        )
        result = get_monthly_revenue_trend(df).orderBy("year_month")
        rows = result.collect()
        assert rows[1]["mom_growth_pct"] == -50.0

    def test_distinct_invoice_count(self, spark: SparkSession) -> None:
        """num_transactions should count distinct invoice numbers."""
        df = _make_cleaned(
            spark,
            [
                # Two rows with the same invoice (two line items in one invoice)
                {"invoice_no": "INV001", "invoice_date": _dt("2011-07-01 10:00:00"), "revenue": 50.0},
                {"invoice_no": "INV001", "invoice_date": _dt("2011-07-01 11:00:00"), "revenue": 30.0},
                {"invoice_no": "INV002", "invoice_date": _dt("2011-07-15 10:00:00"), "revenue": 80.0},
            ],
        )
        result = get_monthly_revenue_trend(df)
        row = result.collect()[0]
        assert row["num_transactions"] == 2  # INV001 and INV002

    def test_result_ordered_by_month(self, spark: SparkSession) -> None:
        df = _make_cleaned(
            spark,
            [
                {"invoice_date": _dt("2011-12-01 10:00:00"), "revenue": 500.0},
                {"invoice_date": _dt("2011-01-01 10:00:00"), "revenue": 100.0},
                {"invoice_date": _dt("2011-06-01 10:00:00"), "revenue": 300.0},
            ],
        )
        result = get_monthly_revenue_trend(df)
        months = [r["year_month"] for r in result.collect()]
        assert months == sorted(months)

    def test_cancellations_reduce_monthly_revenue(self, spark: SparkSession) -> None:
        """Cancellations (negative revenue) must be subtracted for net monthly revenue."""
        df = _make_cleaned(
            spark,
            [
                {"invoice_date": _dt("2011-06-01 10:00:00"), "revenue": 300.0, "is_cancellation": False},
                {"invoice_date": _dt("2011-06-15 10:00:00"), "revenue": -100.0, "is_cancellation": True},
            ],
        )
        result = get_monthly_revenue_trend(df)
        assert result.count() == 1
        assert result.collect()[0]["monthly_revenue"] == 200.0
