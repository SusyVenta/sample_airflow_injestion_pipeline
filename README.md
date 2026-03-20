# Online Retail Data Pipeline

End-to-end data pipeline using **Apache Spark**, **Apache Airflow**, and **PostgreSQL**, orchestrated with Docker Compose.

---

## Project structure

```
.
├── airflow/
│   └── Dockerfile                  # Custom Airflow image (Java 17 + PySpark + providers)
├── dags/
│   └── retail_pipeline_dag.py      # Airflow DAG (daily schedule)
├── data/
│   └── retails.csv                 # Raw dataset (place here before running)
├── spark/
│   └── jobs/
│       ├── clean_and_ingest.py     # PySpark: cleaning, PII anonymisation, PostgreSQL write
│       └── analysis.py             # PySpark: total revenue, top-10 products, monthly trend
├── sql/
│   ├── init_db.sh                      # Creates airflow + retail databases on first Postgres start
│   ├── top_3_products_last_6m.sql      # SQL: top-3 products by revenue per month (last 6 months)
│   ├── rolling_3m_avg_australia.sql    # SQL: rolling 3-month average revenue for Australia
├── tests/
│   ├── conftest.py                 # Shared SparkSession fixture + helpers
│   ├── test_cleaning.py            # Unit tests for cleaning functions
│   └── test_analysis.py            # Unit tests for analysis functions
├── docker-compose.yml              # Postgres 15, Spark 3.5 master+worker, custom Airflow (webserver + scheduler + init)
└── README.md
```

---

## Prerequisites

## 1 — Install Docker

### macOS - tested v. 29.2.1

Download Docker Desktop directly from https://www.docker.com/products/docker-desktop/

Then open **Docker.app** from `/Applications` and complete the first-run setup.

```bash
docker --version            # Docker version 29.2.1, build a5c7197        
docker compose version      # Docker Compose version v5.1.0
```

## 2 — Build images

```bash
docker compose build
```

This builds the custom Airflow image (adds OpenJDK 17, PySpark 3.5, and the Spark + Postgres Airflow providers on top of `apache/airflow:2.9.3`).

> **Apple Silicon (M1/M2/M3) note:** The Dockerfile creates an arch-independent
> `JAVA_HOME` symlink at build time, so the image works correctly on both
> `arm64` and `amd64` hosts without any changes.

---

## 3 — Start infrastructure services

```bash
docker compose up -d postgres spark-master spark-worker && sleep 12 && docker compose ps
```

---

## 4 — Initialise Airflow

Run once to create the Airflow metadata schema and the `admin` user:

```bash
docker compose run --rm airflow-init
```

Expected output ends with: `[airflow-init] Initialisation complete.`

---

## 5 — Start Airflow services

```bash
docker compose up -d airflow-webserver airflow-scheduler
```

| Service | URL |
|---------|-----|
| Airflow UI | http://localhost:8090 (user: `admin` / password: `admin`) |
| Spark master UI | http://localhost:8080 |
| PostgreSQL | `localhost:5432` (superuser: `postgres` / `postgres`) |

---

## 6 — Trigger the pipeline

### Via the Airflow UI
1. Open http://localhost:8090
2. Unpause the `retail_pipeline` DAG (toggle on the left).
3. Click **Trigger DAG** (play button).

### Via the CLI

```bash
docker compose exec airflow-scheduler \
    airflow dags trigger retail_pipeline
```

---

## 7 — Monitor execution

```bash
# Live scheduler logs
docker compose logs -f airflow-scheduler

# Individual task logs are available in the Airflow UI under
# DAGs → retail_pipeline → <run> → <task> → Logs
```

---

## 8 — Inspect results in PostgreSQL

Connect with any PostgreSQL client (e.g. `psql`, DBeaver, or the CLI below):

```bash
docker compose exec postgres \
    psql -U retail -d retail
```

Useful queries after the pipeline has run:

```sql
-- Cleaned transactions
SELECT COUNT(*), COUNT(DISTINCT invoice_no), MIN(invoice_date), MAX(invoice_date)
FROM retail_transactions;

-- Top 10 products saved by PySpark analysis job
SELECT * FROM analysis_top10_products ORDER BY total_quantity DESC;

-- Monthly revenue trend
SELECT * FROM analysis_monthly_revenue ORDER BY year_month;

-- Run the SQL analysis queries directly
\i /path/to/sql/analysis.sql
```

---

## 9 — Run SQL analysis queries manually

```bash
docker compose exec postgres \
    psql -U retail -d retail \
    -f /docker-entrypoint-initdb.d/../sql/analysis.sql
```

Or copy-paste from [sql/analysis.sql](sql/analysis.sql) into any PostgreSQL client connected to the `retail` database.

---

## 10 — Run unit tests inside Docker (recommended)

The `tests` service reuses the custom Airflow image (Java 17 + PySpark 3.5 +
pytest already installed). No local Python/Java setup required.

**Step 1 — build the image** (skip if you already ran `docker compose build`):

```bash
docker compose --profile test build tests
```

**Step 2 — run the tests:**

```bash
docker compose --profile test run --rm tests
```

Expected output ends with something like:

```
============================== 56 passed in 8.62s ==============================
```

**Run with coverage report:**

```bash
docker compose --profile test run --rm tests \
    python -m pytest -v --tb=short \
    --cov=spark/jobs --cov-report=term-missing
```

**Re-run a single test class or file:**

```bash
# Single file
docker compose --profile test run --rm tests \
    python -m pytest tests/test_cleaning.py -v

# Single test class
docker compose --profile test run --rm tests \
    python -m pytest tests/test_cleaning.py::TestCleanDataIntegration -v
```

The tests run PySpark in **local mode** — no Spark cluster or PostgreSQL
connection needed.

---

## 11 — Run integration tests (end-to-end DAG)

The integration tests trigger the full `retail_pipeline` DAG against the live stack, wait for it to complete, then verify the output in PostgreSQL.

**Prerequisites:** the full stack must be running (steps 3–5 completed).

```bash
docker compose --profile integration-test run --rm integration-tests
```

The test runner:
1. Cancels any lingering active DAG runs (avoids `max_active_runs=1` blocking)
2. Triggers a fresh manual run
3. Polls every 15 s until the DAG succeeds or fails (timeout: 15 min)
4. Asserts all 4 tasks succeeded
5. Queries PostgreSQL to verify `retail_transactions`, `analysis_top10_products`, and `analysis_monthly_revenue`

Expected output ends with:

```
============================== 16 passed in XX.XXs ==============================
```

---

## Tear down

```bash
# Stop and remove containers (keeps volumes / data)
docker compose down

# Stop and remove everything including volumes
docker compose down -v
```

---

## Architecture overview

```
                  ┌──────────────────────────────────────────┐
                  │              docker network               │
                  │                                          │
  retails.csv ──► │  airflow-scheduler                       │
  (./data/)       │    └─► SparkSubmitOperator               │
                  │          └─► spark-master:7077            │
                  │                └─► spark-worker           │
                  │                      │                    │
                  │            (JDBC write / read)            │
                  │                      │                    │
                  │                 postgres                  │
                  │                  ├─ airflow DB            │
                  │                  └─ retail DB             │
                  └──────────────────────────────────────────┘
```

### Pipeline DAG

```
ingest_and_clean  ──► run_pyspark_analysis
                  ──► sql_top_3_products_last_6m
                  ──► sql_rolling_3m_avg_australia
```

| Task | Tool | Description |
|------|------|-------------|
| `ingest_and_clean` | SparkSubmitOperator | Reads CSV, cleans data, anonymises CustomerID (PII), writes to `retail_transactions` |
| `run_pyspark_analysis` | SparkSubmitOperator | Reads from PostgreSQL; computes total revenue, top-10 products, monthly trend |
| `sql_top_3_products_last_6m` | PostgresOperator | Window function: top 3 products by revenue per month (last 6 months of data) |
| `sql_rolling_3m_avg_australia` | PostgresOperator | Rolling 3-month average revenue for Australia |

---

## Data cleaning decisions

| Issue | Action |
|-------|--------|
| Missing `InvoiceNo` | Drop row (cannot identify transaction) |
| Missing `Quantity` | Drop row (cannot calculate revenue) |
| Missing `InvoiceDate` | Drop row (required for all time-based analysis) |
| Missing `UnitPrice` | Drop row (required for revenue) |
| Missing `StockCode` | Fill with `UNKNOWN` |
| Missing `Country` | Fill with `Unknown` |
| Missing `CustomerID` | Hash as `ANONYMOUS` |
| `InvoiceNo` starts with `C` | Flag `is_cancellation = True`; kept in table, filtered in analysis |
| Float representation artifacts (`82804.0`, `16016.0`) | Strip `.0` suffix before use / hashing |
| Floating-point revenue drift | Recompute as `ROUND(Quantity * UnitPrice, 2)` |
| Negative `UnitPrice` | Kept; filtered by `revenue > 0` in analysis |
| Duplicate rows | Removed with `dropDuplicates()` |
| `CustomerID` (PII) | Irreversibly anonymised with SHA-256 |

---

## Environment variables reference

All variables have defaults that work out of the box with docker-compose.

| Variable | Default | Description |
|----------|---------|-------------|
| `CSV_PATH` | `/opt/airflow/data/retails.csv` | Path to raw CSV inside containers |
| `POSTGRES_HOST` | `postgres` | PostgreSQL hostname |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `retail` | Retail database name |
| `POSTGRES_USER` | `retail` | Retail database user |
| `POSTGRES_PASSWORD` | `retail` | Retail database password |
