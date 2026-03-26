"""
test_cleaning.py
----------------
Unit tests for every cleaning transformation in clean_and_ingest.py.

All tests use a local SparkSession (no external services required).
Each function under test is imported directly and applied to small,
hand-crafted DataFrames so that behaviour is fully deterministic.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType

from tests.conftest import make_raw_rows

# Import functions under test
from spark.jobs.clean_and_ingest import (
    RETAIL_CSV_SCHEMA,
    anonymise_customer_id,
    cast_types,
    clean_data,
    clean_stock_code,
    drop_invalid_rows,
    fill_missing_country,
    flag_cancellations,
    load_raw_data,
    recalculate_revenue,
    rename_to_snake_case,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _collect_col(df: DataFrame, col: str) -> list:
    return [row[col] for row in df.collect()]


# ---------------------------------------------------------------------------
# drop_invalid_rows
# ---------------------------------------------------------------------------


class TestDropInvalidRows:
    def test_keeps_valid_row(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{}])
        result = drop_invalid_rows(df)
        assert result.count() == 1

    def test_drops_null_invoice_no(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"InvoiceNo": None}])
        assert drop_invalid_rows(df).count() == 0

    def test_drops_empty_string_invoice_no(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"InvoiceNo": ""}])
        assert drop_invalid_rows(df).count() == 0

    def test_drops_whitespace_only_invoice_no(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"InvoiceNo": "   "}])
        assert drop_invalid_rows(df).count() == 0

    def test_drops_null_quantity(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"Quantity": None}])
        assert drop_invalid_rows(df).count() == 0

    def test_drops_null_invoice_date(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"InvoiceDate": None}])
        assert drop_invalid_rows(df).count() == 0

    def test_drops_null_unit_price(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"UnitPrice": None}])
        assert drop_invalid_rows(df).count() == 0

    def test_keeps_cancellation_row(self, spark: SparkSession) -> None:
        """Cancellations are dropped only by analysis filters, not by cleaning."""
        df = make_raw_rows(spark, [{"InvoiceNo": "C536381"}])
        assert drop_invalid_rows(df).count() == 1

    def test_drops_multiple_invalid_rows(self, spark: SparkSession) -> None:
        df = make_raw_rows(
            spark,
            [
                {},                         # valid
                {"InvoiceNo": None},        # invalid
                {"Quantity": None},         # invalid
                {"InvoiceDate": None},      # invalid
            ],
        )
        assert drop_invalid_rows(df).count() == 1


# ---------------------------------------------------------------------------
# flag_cancellations
# ---------------------------------------------------------------------------


class TestFlagCancellations:
    def test_flags_cancellation(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"InvoiceNo": "C536381"}])
        result = flag_cancellations(df)
        assert _collect_col(result, "is_cancellation") == [True]

    def test_normal_invoice_not_flagged(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"InvoiceNo": "536381"}])
        result = flag_cancellations(df)
        assert _collect_col(result, "is_cancellation") == [False]

    def test_mixed_invoices(self, spark: SparkSession) -> None:
        df = make_raw_rows(
            spark,
            [{"InvoiceNo": "536381"}, {"InvoiceNo": "C536382"}],
        )
        flags = _collect_col(flag_cancellations(df).orderBy("InvoiceNo"), "is_cancellation")
        assert flags == [False, True]


# ---------------------------------------------------------------------------
# clean_stock_code
# ---------------------------------------------------------------------------


class TestCleanStockCode:
    def test_null_becomes_unknown(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"StockCode": None}])
        result = clean_stock_code(df)
        assert _collect_col(result, "StockCode") == ["UNKNOWN"]

    def test_empty_becomes_unknown(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"StockCode": ""}])
        result = clean_stock_code(df)
        assert _collect_col(result, "StockCode") == ["UNKNOWN"]

    def test_float_representation_stripped(self, spark: SparkSession) -> None:
        """'82804.0' (float CSV inference artifact) should become '82804'."""
        df = make_raw_rows(spark, [{"StockCode": "82804.0"}])
        result = clean_stock_code(df)
        assert _collect_col(result, "StockCode") == ["82804"]

    def test_valid_code_preserved(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"StockCode": "85123"}])
        result = clean_stock_code(df)
        assert _collect_col(result, "StockCode") == ["85123"]

    def test_alpha_code_preserved(self, spark: SparkSession) -> None:
        """Non-numeric stock codes (e.g. 'POST') must not be altered."""
        df = make_raw_rows(spark, [{"StockCode": "POST"}])
        result = clean_stock_code(df)
        assert _collect_col(result, "StockCode") == ["POST"]


# ---------------------------------------------------------------------------
# fill_missing_country
# ---------------------------------------------------------------------------


class TestFillMissingCountry:
    def test_null_country_filled(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"Country": None}])
        result = fill_missing_country(df)
        assert _collect_col(result, "Country") == ["Unknown"]

    def test_existing_country_preserved(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"Country": "France"}])
        result = fill_missing_country(df)
        assert _collect_col(result, "Country") == ["France"]


# ---------------------------------------------------------------------------
# anonymise_customer_id
# ---------------------------------------------------------------------------


class TestAnonymiseCustomerId:
    def test_null_customer_id_becomes_anonymous(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"CustomerID": None}])
        result = anonymise_customer_id(df)
        assert _collect_col(result, "CustomerID") == ["ANONYMOUS"]

    def test_customer_id_is_hashed(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"CustomerID": "17850"}])
        result = anonymise_customer_id(df)
        expected_hash = _sha256("17850")
        assert _collect_col(result, "CustomerID") == [expected_hash]

    def test_float_suffix_stripped_before_hashing(self, spark: SparkSession) -> None:
        """'17850.0' and '17850' must hash to the same value."""
        df_with_suffix = make_raw_rows(spark, [{"CustomerID": "17850.0"}])
        df_without = make_raw_rows(spark, [{"CustomerID": "17850"}])
        hash_with = _collect_col(anonymise_customer_id(df_with_suffix), "CustomerID")[0]
        hash_without = _collect_col(anonymise_customer_id(df_without), "CustomerID")[0]
        assert hash_with == hash_without

    def test_hash_is_deterministic(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"CustomerID": "12345"}])
        result1 = _collect_col(anonymise_customer_id(df), "CustomerID")[0]
        result2 = _collect_col(anonymise_customer_id(df), "CustomerID")[0]
        assert result1 == result2

    def test_different_ids_produce_different_hashes(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"CustomerID": "11111"}, {"CustomerID": "22222"}])
        hashes = _collect_col(anonymise_customer_id(df), "CustomerID")
        assert hashes[0] != hashes[1]


# ---------------------------------------------------------------------------
# recalculate_revenue
# ---------------------------------------------------------------------------


class TestRecalculateRevenue:
    def test_revenue_is_quantity_times_price(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"Quantity": 10.0, "UnitPrice": 2.55, "Revenue": 999.0}])
        result = recalculate_revenue(df)
        assert _collect_col(result, "Revenue") == [25.50]

    def test_negative_unit_price_gives_negative_revenue(self, spark: SparkSession) -> None:
        """Negative unit price is kept; analysis layers filter by revenue > 0."""
        df = make_raw_rows(spark, [{"Quantity": 14.0, "UnitPrice": -29.81, "Revenue": 0.0}])
        result = recalculate_revenue(df)
        # 14 * -29.81 = -417.34
        assert _collect_col(result, "Revenue") == [-417.34]

    def test_revenue_rounded_to_two_decimals(self, spark: SparkSession) -> None:
        # 3 * 1.005 = 3.015 → should round to 3.02
        df = make_raw_rows(spark, [{"Quantity": 3.0, "UnitPrice": 1.005, "Revenue": 0.0}])
        result = recalculate_revenue(df)
        assert _collect_col(result, "Revenue") == [round(3 * 1.005, 2)]

    def test_floating_point_drift_corrected(self, spark: SparkSession) -> None:
        """Source had 1697.4499999999998; we expect 1697.45 after recalculation."""
        df = make_raw_rows(
            spark,
            [{"Quantity": 85.0, "UnitPrice": 19.97, "Revenue": 1697.4499999999998}],
        )
        result = recalculate_revenue(df)
        assert _collect_col(result, "Revenue") == [round(85 * 19.97, 2)]


# ---------------------------------------------------------------------------
# clean_data (integration test)
# ---------------------------------------------------------------------------


class TestCleanDataIntegration:
    def test_duplicates_removed(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{}, {}])  # identical rows
        assert clean_data(df).count() == 1

    def test_invalid_rows_removed_and_valid_rows_kept(self, spark: SparkSession) -> None:
        df = make_raw_rows(
            spark,
            [
                {},                     # valid → kept
                {"InvoiceNo": None},    # invalid → dropped
                {"Quantity": None},     # invalid → dropped
            ],
        )
        assert clean_data(df).count() == 1

    def test_output_columns_are_snake_case(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{}])
        result = clean_data(df)
        assert "invoice_no" in result.columns
        assert "stock_code" in result.columns
        assert "customer_id" in result.columns
        assert "is_cancellation" in result.columns
        # Original CamelCase names must not be present
        assert "InvoiceNo" not in result.columns
        assert "CustomerID" not in result.columns

    def test_cancellation_flag_set(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"InvoiceNo": "C536381"}])
        result = clean_data(df)
        assert _collect_col(result, "is_cancellation") == [True]

    def test_customer_id_anonymised(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"CustomerID": "17850"}])
        result = clean_data(df)
        hashed = _collect_col(result, "customer_id")[0]
        # Must not be the original value
        assert hashed != "17850"
        # Must be a 64-char hex SHA-256 digest
        assert len(hashed) == 64
        assert all(c in "0123456789abcdef" for c in hashed)

    def test_missing_country_filled(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"Country": None}])
        result = clean_data(df)
        assert _collect_col(result, "country") == ["Unknown"]

    def test_missing_stock_code_filled(self, spark: SparkSession) -> None:
        df = make_raw_rows(spark, [{"StockCode": None}])
        result = clean_data(df)
        assert _collect_col(result, "stock_code") == ["UNKNOWN"]

    def test_revenue_recalculated(self, spark: SparkSession) -> None:
        df = make_raw_rows(
            spark, [{"Quantity": 10.0, "UnitPrice": 3.00, "Revenue": 999.0}]
        )
        result = clean_data(df)
        assert _collect_col(result, "revenue") == [30.0]


# ---------------------------------------------------------------------------
# load_raw_data
# ---------------------------------------------------------------------------

# Minimal CSV that exercises both schema modes.
_CSV_CONTENT = (
    "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,"
    "UnitPrice,CustomerID,Country,Revenue\n"
    "536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,"
    "6,12/1/2010 8:26,2.55,17850,United Kingdom,15.3\n"
    "536366,71053,WHITE METAL LANTERN,"
    "6,12/1/2010 8:26,3.39,,France,20.34\n"
)


class TestLoadRawData:
    def _csv(self, tmp_path: Path) -> str:
        p = tmp_path / "retail.csv"
        p.write_text(_CSV_CONTENT)
        return str(p)

    def test_enforce_schema_loads_all_rows(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        df = load_raw_data(spark, self._csv(tmp_path), enforce_schema=True)
        assert df.count() == 2

    def test_infer_schema_loads_all_rows(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        df = load_raw_data(spark, self._csv(tmp_path), enforce_schema=False)
        assert df.count() == 2

    def test_enforce_schema_is_default(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """Calling load_raw_data with no keyword must behave like enforce_schema=True."""
        path = self._csv(tmp_path)
        assert load_raw_data(spark, path).schema == load_raw_data(
            spark, path, enforce_schema=True
        ).schema

    def test_enforce_schema_matches_retail_csv_schema(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        df = load_raw_data(spark, self._csv(tmp_path), enforce_schema=True)
        assert df.schema == RETAIL_CSV_SCHEMA

    def test_enforce_schema_customer_id_is_string(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """With enforced schema CustomerID is StringType — no .0 float artifact."""
        df = load_raw_data(spark, self._csv(tmp_path), enforce_schema=True)
        assert df.schema["CustomerID"].dataType == StringType()

    def test_enforce_schema_customer_id_no_float_suffix(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """CustomerID '17850' must arrive as '17850', not '17850.0'."""
        df = load_raw_data(spark, self._csv(tmp_path), enforce_schema=True)
        ids = [r["CustomerID"] for r in df.collect() if r["CustomerID"] is not None]
        assert all("." not in cid for cid in ids)

    def test_enforce_schema_invoice_date_is_string(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """InvoiceDate is loaded as StringType so cast_types() can handle the format."""
        df = load_raw_data(spark, self._csv(tmp_path), enforce_schema=True)
        assert df.schema["InvoiceDate"].dataType == StringType()

    def test_enforce_schema_null_customer_id_preserved(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """Rows with an empty CustomerID column must yield NULL, not an empty string."""
        df = load_raw_data(spark, self._csv(tmp_path), enforce_schema=True)
        # Second row has no CustomerID in the test CSV
        null_count = df.filter(df["CustomerID"].isNull()).count()
        assert null_count == 1
