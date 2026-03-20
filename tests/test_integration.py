"""
test_integration.py
-------------------
End-to-end integration tests for the retail_pipeline DAG.

Requires the full Docker Compose stack to be running:
  docker compose up -d postgres spark-master spark-worker
  docker compose run --rm airflow-init
  docker compose up -d airflow-webserver airflow-scheduler

Run via Docker (recommended – no local Python setup needed):
  docker compose --profile integration-test run --rm integration-tests

Run directly against a running stack (needs requests + psycopg2 installed locally):
  AIRFLOW_BASE_URL=http://localhost:8090 POSTGRES_HOST=localhost \
      python -m pytest tests/test_integration.py -v

The module-scoped fixture `dag_run_id` triggers the DAG once, waits up to
10 minutes for it to finish, then all test classes reuse the same run.
"""

from __future__ import annotations

import os
import time

import psycopg2
import pytest
import requests

# ---------------------------------------------------------------------------
# Connection configuration (defaults work inside the Docker Compose network)
# ---------------------------------------------------------------------------

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
AIRFLOW_AUTH = ("admin", "admin")
DAG_ID = "retail_pipeline"

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "retail")
PG_USER = os.getenv("POSTGRES_USER", "retail")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "retail")

POLL_INTERVAL = 15   # seconds between status checks
TIMEOUT = 900        # maximum seconds to wait for the DAG to finish (15 min)

# Airflow DAG run states that count as "still active" (block new runs)
_ACTIVE_STATES = {"queued", "running", "up_for_retry"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _airflow_get(path: str) -> dict:
    resp = requests.get(
        f"{AIRFLOW_BASE_URL}/api/v1{path}",
        auth=AIRFLOW_AUTH,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _airflow_patch(path: str, payload: dict) -> dict:
    resp = requests.patch(
        f"{AIRFLOW_BASE_URL}/api/v1{path}",
        auth=AIRFLOW_AUTH,
        json=payload,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _airflow_post(path: str, payload: dict | None = None) -> dict:
    resp = requests.post(
        f"{AIRFLOW_BASE_URL}/api/v1{path}",
        auth=AIRFLOW_AUTH,
        json=payload or {},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _pg_scalar(sql: str) -> object:
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]
    finally:
        conn.close()


def _pg_col(sql: str) -> list:
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Module-scoped fixture: trigger DAG once and wait for it to succeed
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def dag_run_id():
    """
    Unpause the DAG, cancel any active runs (so max_active_runs=1 doesn't
    block us), trigger a fresh manual run, poll until it succeeds or fails,
    and return the run_id.  All tests in this module share this one run.
    """
    # Unpause (DAGs are paused on creation by default)
    _airflow_patch(f"/dags/{DAG_ID}", {"is_paused": False})

    # Cancel any runs that are still active so ours can start immediately
    existing = _airflow_get(f"/dags/{DAG_ID}/dagRuns?limit=25")
    for run in existing.get("dag_runs", []):
        if run["state"] in _ACTIVE_STATES:
            print(f"\n[integration] Cancelling active run: {run['dag_run_id']} ({run['state']})")
            try:
                _airflow_patch(
                    f"/dags/{DAG_ID}/dagRuns/{run['dag_run_id']}",
                    {"state": "failed"},
                )
            except Exception:
                pass  # best-effort
    time.sleep(3)  # let Airflow process the cancellations

    # Trigger a fresh manual run
    run_data = _airflow_post(f"/dags/{DAG_ID}/dagRuns")
    run_id = run_data["dag_run_id"]

    print(f"\n[integration] DAG run triggered: {run_id}")
    print(f"[integration] Waiting up to {TIMEOUT}s for completion …")

    deadline = time.time() + TIMEOUT
    while time.time() < deadline:
        status = _airflow_get(f"/dags/{DAG_ID}/dagRuns/{run_id}")
        state = status["state"]
        print(f"[integration] DAG state: {state}")
        if state == "success":
            print("[integration] DAG run completed successfully.")
            return run_id
        if state in ("failed", "cancelled"):
            # Collect task states for a useful failure message
            tasks = _airflow_get(
                f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances"
            )
            failed = [
                f"{ti['task_id']}={ti['state']}"
                for ti in tasks["task_instances"]
                if ti["state"] not in ("success",)
            ]
            pytest.fail(
                f"DAG run ended with state '{state}'. "
                f"Non-success tasks: {failed}"
            )
        time.sleep(POLL_INTERVAL)

    pytest.fail(f"DAG did not complete within {TIMEOUT} seconds.")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDAGCompletion:
    def test_dag_run_succeeded(self, dag_run_id):
        status = _airflow_get(f"/dags/{DAG_ID}/dagRuns/{dag_run_id}")
        assert status["state"] == "success"

    def test_all_tasks_succeeded(self, dag_run_id):
        tasks = _airflow_get(
            f"/dags/{DAG_ID}/dagRuns/{dag_run_id}/taskInstances"
        )
        for ti in tasks["task_instances"]:
            assert ti["state"] == "success", (
                f"Task '{ti['task_id']}' ended in state '{ti['state']}'"
            )


class TestRetailTransactions:
    """Verifies the output of the ingest_and_clean Spark job."""

    def test_table_has_rows(self, dag_run_id):
        count = _pg_scalar("SELECT COUNT(*) FROM retail_transactions")
        assert count > 0, "retail_transactions is empty"

    def test_no_null_invoice_no(self, dag_run_id):
        bad = _pg_scalar(
            "SELECT COUNT(*) FROM retail_transactions "
            "WHERE invoice_no IS NULL OR invoice_no = ''"
        )
        assert bad == 0

    def test_no_null_invoice_date(self, dag_run_id):
        bad = _pg_scalar(
            "SELECT COUNT(*) FROM retail_transactions WHERE invoice_date IS NULL"
        )
        assert bad == 0

    def test_customer_ids_anonymised(self, dag_run_id):
        """Every non-ANONYMOUS customer_id must be a 64-char hex SHA-256 hash."""
        bad = _pg_scalar(
            "SELECT COUNT(*) FROM retail_transactions "
            "WHERE customer_id <> 'ANONYMOUS' AND LENGTH(customer_id) <> 64"
        )
        assert bad == 0

    def test_cancellations_flagged(self, dag_run_id):
        """Dataset contains cancellations; is_cancellation must be used."""
        cancellations = _pg_scalar(
            "SELECT COUNT(*) FROM retail_transactions WHERE is_cancellation = TRUE"
        )
        assert cancellations > 0

    def test_no_duplicate_rows(self, dag_run_id):
        total = _pg_scalar("SELECT COUNT(*) FROM retail_transactions")
        distinct = _pg_scalar(
            "SELECT COUNT(*) FROM ("
            "  SELECT DISTINCT invoice_no, stock_code, quantity, invoice_date, "
            "                  unit_price, customer_id, country "
            "  FROM retail_transactions"
            ") t"
        )
        assert total == distinct, "Duplicate rows found in retail_transactions"


class TestAnalysisTop10Products:
    """Verifies the output of the run_pyspark_analysis Spark job."""

    def test_table_has_rows(self, dag_run_id):
        count = _pg_scalar("SELECT COUNT(*) FROM analysis_top10_products")
        assert count > 0

    def test_at_most_10_rows(self, dag_run_id):
        count = _pg_scalar("SELECT COUNT(*) FROM analysis_top10_products")
        assert count <= 10

    def test_quantities_are_positive(self, dag_run_id):
        bad = _pg_scalar(
            "SELECT COUNT(*) FROM analysis_top10_products WHERE quantity_sold <= 0"
        )
        assert bad == 0


class TestAnalysisMonthlyRevenue:
    """Verifies the monthly revenue trend output."""

    def test_table_has_rows(self, dag_run_id):
        count = _pg_scalar("SELECT COUNT(*) FROM analysis_monthly_revenue")
        assert count > 0

    def test_monthly_revenue_is_positive(self, dag_run_id):
        bad = _pg_scalar(
            "SELECT COUNT(*) FROM analysis_monthly_revenue WHERE monthly_revenue <= 0"
        )
        assert bad == 0

    def test_ordered_by_month(self, dag_run_id):
        months = _pg_col(
            "SELECT year_month FROM analysis_monthly_revenue ORDER BY year_month"
        )
        assert months == sorted(months)

    def test_first_month_has_null_mom_growth(self, dag_run_id):
        """The earliest month has no prior month, so mom_growth_pct must be NULL."""
        null_count = _pg_scalar(
            "SELECT COUNT(*) FROM analysis_monthly_revenue "
            "WHERE year_month = (SELECT MIN(year_month) FROM analysis_monthly_revenue) "
            "  AND mom_growth_pct IS NULL"
        )
        assert null_count == 1
