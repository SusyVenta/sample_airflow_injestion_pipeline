"""
retail_pipeline_dag.py
----------------------
Airflow DAG that orchestrates the Online Retail data pipeline on a daily
schedule.

Pipeline graph:
                                    ┌─► run_pyspark_analysis
    ingest_and_clean ───────────────┤
                                    ├─► sql_top_3_products_last_6m
                                    └─► sql_rolling_3m_avg_australia

Tasks:
  1. ingest_and_clean            – SparkSubmitOperator: clean_and_ingest.py
     Reads the raw CSV, applies the cleaning pipeline, anonymises CustomerID
     (PII), and writes the result to PostgreSQL.

  2. run_pyspark_analysis        – SparkSubmitOperator: analysis.py
     Reads from PostgreSQL and computes total revenue, top-10 products, and
     the monthly revenue trend.  Results are written back to analysis tables.

  3. sql_top_3_products_last_6m  – PostgresOperator
     Runs the window-function query that returns the top 3 products by
     revenue for each month over the last 6 months of data.

  4. sql_rolling_3m_avg_australia – PostgresOperator
     Computes the rolling 3-month average revenue for Australia.

Connections expected in Airflow (set via env vars in docker-compose):
  • spark_default   → spark://spark-master:7077
  • postgres_retail → postgresql+psycopg2://retail:retail@postgres:5432/retail
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ---------------------------------------------------------------------------
# Connection IDs (must match the Airflow connections configured in
# docker-compose via AIRFLOW_CONN_* environment variables)
# ---------------------------------------------------------------------------
SPARK_CONN_ID: str = "spark_default"
POSTGRES_CONN_ID: str = "postgres_retail"

# Path to PySpark job scripts inside the Airflow container
SPARK_JOBS_DIR: str = "/opt/airflow/spark/jobs"

# Maven coordinates for the PostgreSQL JDBC driver (downloaded at submit time)
JDBC_PACKAGE: str = "org.postgresql:postgresql:42.7.1"

# ---------------------------------------------------------------------------
# Default task arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # Retry twice with a 5-minute back-off before marking the task failed
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Embedded SQL for the PostgresOperator tasks
# (mirrors what is in sql/analysis.sql for pipeline-internal use)
# ---------------------------------------------------------------------------

_SQL_TOP_3_PRODUCTS = """
WITH dataset_max_date AS (
    SELECT MAX(invoice_date) AS max_dt
    FROM   retail_transactions
),
last_6_months_window AS (
    SELECT max_dt,
           max_dt - INTERVAL '6 months' AS window_start
    FROM   dataset_max_date
),
monthly_product_revenue AS (
    SELECT
        DATE_TRUNC('month', t.invoice_date)::DATE  AS month,
        t.stock_code,
        t.description,
        SUM(t.revenue)                             AS total_revenue
    FROM   retail_transactions t
    CROSS  JOIN last_6_months_window w
    WHERE  t.invoice_date  >  w.window_start
      AND  t.invoice_date  <= w.max_dt
      AND  t.is_cancellation = FALSE
      AND  t.revenue         > 0
    GROUP  BY 1, 2, 3
),
ranked AS (
    SELECT
        month,
        stock_code,
        description,
        total_revenue,
        RANK() OVER (
            PARTITION BY month
            ORDER BY     total_revenue DESC
        ) AS revenue_rank
    FROM  monthly_product_revenue
)
SELECT
    TO_CHAR(month, 'YYYY-MM')        AS month,
    stock_code,
    description,
    ROUND(total_revenue::NUMERIC, 2) AS total_revenue_gbp,
    revenue_rank
FROM   ranked
WHERE  revenue_rank <= 3
ORDER  BY month DESC, revenue_rank;
"""

_SQL_ROLLING_AVG_AUSTRALIA = """
WITH monthly_australia AS (
    SELECT
        DATE_TRUNC('month', invoice_date)::DATE  AS month,
        SUM(revenue)                             AS monthly_revenue
    FROM   retail_transactions
    WHERE  country         = 'Australia'
      AND  is_cancellation = FALSE
      AND  revenue         > 0
    GROUP  BY 1
)
SELECT
    TO_CHAR(month, 'YYYY-MM')                        AS month,
    ROUND(monthly_revenue::NUMERIC, 2)               AS monthly_revenue_gbp,
    ROUND(
        AVG(monthly_revenue) OVER (
            ORDER BY month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::NUMERIC,
        2
    )                                                AS rolling_3m_avg_gbp
FROM   monthly_australia
ORDER  BY month;
"""

# ---------------------------------------------------------------------------
# Shared Spark configuration
# ---------------------------------------------------------------------------
_SPARK_CONF = {
    # In client mode the driver runs inside the Airflow container; the
    # host name must be resolvable by Spark workers so they can connect back.
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.executor.memory": "2g",
    "spark.driver.memory": "2g",
}

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
    sql_top_products = PostgresOperator(
        task_id="sql_top_3_products_last_6m",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=_SQL_TOP_3_PRODUCTS,
    )

    # ------------------------------------------------------------------
    # Task 4 – SQL: rolling 3-month average revenue for Australia
    # ------------------------------------------------------------------
    sql_rolling_avg = PostgresOperator(
        task_id="sql_rolling_3m_avg_australia",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=_SQL_ROLLING_AVG_AUSTRALIA,
    )

    # ------------------------------------------------------------------
    # Dependencies
    # Cleaning must finish before any downstream analysis task.
    # PySpark analysis and both SQL tasks can run in parallel afterwards.
    # ------------------------------------------------------------------
    ingest_and_clean >> [run_pyspark_analysis, sql_top_products, sql_rolling_avg]
