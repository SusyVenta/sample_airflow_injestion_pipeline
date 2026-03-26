"""
retail_pipeline_dag.py
----------------------
Airflow DAG that orchestrates the Online Retail data pipeline on a daily
schedule.

Pipeline graph:

                                         ┌─► run_pyspark_analysis
    create_tables ──► ingest_and_clean ──┤
                                         ├─► sql_top_3_products_last_6m
                                         └─► sql_rolling_3m_avg_australia

Tasks:
  0. create_tables               – PythonOperator
     Applies every DDL file in sql/ddl/ (CREATE TABLE IF NOT EXISTS) so all
     output tables exist with precise column types before any data is written.

  1. ingest_and_clean            – SparkSubmitOperator: clean_and_ingest.py
     Reads the raw CSV, applies the cleaning pipeline, anonymises CustomerID
     (PII), and appends the result to retail_transactions.

  2. run_pyspark_analysis        – SparkSubmitOperator: analysis.py
     Reads the latest retail_transactions batch from PostgreSQL, computes
     total revenue, top-10 products, and monthly revenue trend, and appends
     results to analysis_top10_products and analysis_monthly_revenue.

  3. sql_top_3_products_last_6m  – PythonOperator
     Reads sql/top_3_products_last_6m.sql, executes it once via psycopg2,
     logs a sample of the result rows, and appends the full result set to
     sql_top_3_products_last_6m.

  4. sql_rolling_3m_avg_australia – PythonOperator
     Reads sql/rolling_3m_avg_australia.sql, executes it once via psycopg2,
     logs a sample of the result rows, and appends the full result set to
     sql_rolling_3m_avg_australia.

Connections expected in Airflow (set via env vars in docker-compose):
  • spark_default   → spark://spark-master:7077
  • postgres_retail → retail database on the postgres service
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---------------------------------------------------------------------------
# Connection IDs (must match the Airflow connections configured in
# docker-compose via AIRFLOW_CONN_* environment variables)
# ---------------------------------------------------------------------------
SPARK_CONN_ID: str = "spark_default"
POSTGRES_CONN_ID: str = "postgres_retail"

# Path to PySpark job scripts inside the Airflow container
SPARK_JOBS_DIR: str = "/opt/airflow/spark/jobs"

# Path to SQL files inside the Airflow container (mounted from ./sql/)
SQL_DIR: Path = Path("/opt/airflow/sql")

# Maven coordinates for the PostgreSQL JDBC driver (downloaded at submit time)
JDBC_PACKAGE: str = "org.postgresql:postgresql:42.7.1"

# Number of sample rows to log after each SQL query executes
_SAMPLE_ROWS: int = 10

_LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Email notifications
# ---------------------------------------------------------------------------
# ALERT_EMAIL is read from the environment (set in docker-compose.yml).
# When non-empty Airflow will email this address on every task failure,
# using the AIRFLOW__SMTP__* settings also defined in docker-compose.yml.
# Set ALERT_EMAIL="" in docker-compose.yml to disable notifications.
_ALERT_EMAIL: str = os.getenv("ALERT_EMAIL", "")

# ---------------------------------------------------------------------------
# Default task arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    # Send an email on task failure if ALERT_EMAIL is configured.
    "email_on_failure": bool(_ALERT_EMAIL),
    "email_on_retry": False,
    "email": [_ALERT_EMAIL] if _ALERT_EMAIL else [],
    # Retry twice with a 5-minute back-off before marking the task failed
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Shared Spark configuration
# ---------------------------------------------------------------------------
_SPARK_CONF = {
    # In client mode the driver runs inside the Airflow container; the
    # host name must be resolvable by Spark workers so they can connect back.
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.executor.memory": "1g",
    "spark.driver.memory": "2g",
    "spark.executor.cores": "1",
    "spark.cores.max": "2",
}

# ---------------------------------------------------------------------------
# DDL helper
# ---------------------------------------------------------------------------


def _create_output_tables(postgres_conn_id: str) -> None:
    """Apply every DDL file in sql/ddl/ via CREATE TABLE IF NOT EXISTS.

    Executed as the first DAG task so all output tables exist with precise
    column types before any data is written by downstream tasks.
    Files are applied in alphabetical order; each file is expected to contain
    a single idempotent CREATE TABLE IF NOT EXISTS statement.
    """
    ddl_dir = SQL_DIR / "ddl"
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    try:
        for ddl_file in sorted(ddl_dir.glob("*.sql")):
            sql = ddl_file.read_text()
            _LOG.info("Applying DDL: %s", ddl_file.name)
            with conn.cursor() as cur:
                cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# SQL task helper
# ---------------------------------------------------------------------------


def _ascii_table(columns: list[str], rows: list[tuple]) -> str:
    """Format *rows* as an ASCII table with *columns* as headers."""
    widths = [len(c) for c in columns]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val)))
    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    header = "|" + "|".join(f" {c:<{w}} " for c, w in zip(columns, widths)) + "|"
    lines = [sep, header, sep]
    for row in rows:
        lines.append("|" + "|".join(f" {str(v):<{w}} " for v, w in zip(row, widths)) + "|")
    lines.append(sep)
    return "\n".join(lines)


def _run_sql_and_log(
    sql_filename: str,
    postgres_conn_id: str,
    target_table: str | None = None,
) -> None:
    """Read *sql_filename* from SQL_DIR, execute it once, log sample rows,
    and optionally persist the full result set to *target_table*.

    Using psycopg2 directly (via PostgresHook) guarantees a single execution
    and gives us access to the cursor so we can fetch and log result rows.
    PostgresOperator would log the query text and then execute it, but does
    not expose the result set for logging.

    When *target_table* is provided the full result set is appended to that
    table (which must already exist, created by the create_tables task).
    A loaded_at timestamp column is added to each row so every pipeline run
    is traceable and the table accumulates a full history.
    """
    sql_path = SQL_DIR / sql_filename
    sql = sql_path.read_text()

    _LOG.info("Executing SQL file: %s", sql_path)

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            # SELECT queries: fetch rows, log a sample, and optionally persist.
            if cur.description is not None:
                columns = [desc[0] for desc in cur.description]
                all_rows = cur.fetchall()
                sample = all_rows[:_SAMPLE_ROWS]
                _LOG.info(
                    "Sample data from %s (first %d of %d row(s)):\n%s",
                    sql_filename,
                    len(sample),
                    len(all_rows),
                    _ascii_table(columns, sample),
                )
                if target_table and all_rows:
                    loaded_at = datetime.now(timezone.utc)
                    rows_with_ts = [row + (loaded_at,) for row in all_rows]
                    col_list = ", ".join(f'"{c}"' for c in columns) + ', "loaded_at"'
                    cur.executemany(
                        f'INSERT INTO "{target_table}" ({col_list}) VALUES'
                        f' ({", ".join(["%s"] * (len(columns) + 1))})',
                        rows_with_ts,
                    )
                    _LOG.info(
                        "Persisted %d row(s) to table '%s'.",
                        len(all_rows),
                        target_table,
                    )
            conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="retail_pipeline",
    description=(
        "Daily pipeline: ingest raw CSV → clean → load PostgreSQL → analyse"
    ),
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    # Do not backfill historical runs on first deployment
    catchup=False,
    tags=["retail", "etl", "pyspark"],
    # Prevent concurrent runs from stomping on each other's PostgreSQL writes
    max_active_runs=1,
) as dag:

    # ------------------------------------------------------------------
    # Task 0 – Create output tables (idempotent DDL)
    # ------------------------------------------------------------------
    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_output_tables,
        op_kwargs={"postgres_conn_id": POSTGRES_CONN_ID},
    )

    # ------------------------------------------------------------------
    # Task 1 – Ingest and clean
    # ------------------------------------------------------------------
    ingest_and_clean = SparkSubmitOperator(
        task_id="ingest_and_clean",
        application=f"{SPARK_JOBS_DIR}/clean_and_ingest.py",
        conn_id=SPARK_CONN_ID,
        packages=JDBC_PACKAGE,
        verbose=False,
        conf=_SPARK_CONF,
    )

    # ------------------------------------------------------------------
    # Task 2 – PySpark analysis (depends on cleaned data being in PG)
    # ------------------------------------------------------------------
    run_pyspark_analysis = SparkSubmitOperator(
        task_id="run_pyspark_analysis",
        application=f"{SPARK_JOBS_DIR}/analysis.py",
        conn_id=SPARK_CONN_ID,
        packages=JDBC_PACKAGE,
        verbose=False,
        conf=_SPARK_CONF,
    )

    # ------------------------------------------------------------------
    # Task 3 – SQL: top 3 products per month (last 6 months)
    # ------------------------------------------------------------------
    sql_top_products = PythonOperator(
        task_id="sql_top_3_products_last_6m",
        python_callable=_run_sql_and_log,
        op_kwargs={
            "sql_filename": "top_3_products_last_6m.sql",
            "postgres_conn_id": POSTGRES_CONN_ID,
            "target_table": "sql_top_3_products_last_6m",
        },
    )

    # ------------------------------------------------------------------
    # Task 4 – SQL: rolling 3-month average revenue for Australia
    # ------------------------------------------------------------------
    sql_rolling_avg = PythonOperator(
        task_id="sql_rolling_3m_avg_australia",
        python_callable=_run_sql_and_log,
        op_kwargs={
            "sql_filename": "rolling_3m_avg_australia.sql",
            "postgres_conn_id": POSTGRES_CONN_ID,
            "target_table": "sql_rolling_3m_avg_australia",
        },
    )

    # ------------------------------------------------------------------
    # Dependencies
    # DDL must run first so tables exist before any data is written.
    # Cleaning must finish before any downstream analysis task.
    # PySpark analysis and both SQL tasks can run in parallel afterwards.
    # ------------------------------------------------------------------
    create_tables >> ingest_and_clean >> [run_pyspark_analysis, sql_top_products, sql_rolling_avg]
