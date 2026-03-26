# Online Retail Data Pipeline

End-to-end data pipeline using **Apache Spark**, **Apache Airflow**, and **PostgreSQL**, orchestrated with Docker Compose. Implements requirements specified in 'Pipeline_requirements.pdf'.

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
│   ├── ddl/                                # DDL files: one CREATE TABLE IF NOT EXISTS per output table
│   │   ├── retail_transactions.sql
│   │   ├── analysis_top10_products.sql
│   │   ├── analysis_monthly_revenue.sql
│   │   ├── sql_top_3_products_last_6m.sql
│   │   └── sql_rolling_3m_avg_australia.sql
│   ├── init_db.sh                      # Creates airflow + retail databases on first Postgres start
│   ├── top_3_products_last_6m.sql      # SQL: top-3 products by revenue per month (last 6 months)
│   ├── rolling_3m_avg_australia.sql    # SQL: rolling 3-month average revenue for Australia
├── tests/
│   ├── conftest.py                 # Shared SparkSession fixture + email notification hook
│   ├── unit/
│   │   ├── test_cleaning.py        # Unit tests for cleaning functions
│   │   └── test_analysis.py        # Unit tests for analysis functions
│   └── integration/
│       └── test_integration.py     # End-to-end DAG tests (requires full stack)
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

- `-d` detached mode. Containers start in the background and your terminal returns immediately.
- `ps`: print status of all running services

---

## 4 — Initialise Airflow

Run once to create the Airflow metadata schema and the `admin` user:

```bash
docker compose run --rm airflow-init
```

- `rm`: remoeves the container once the one-off setup task is done running

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
-- List all tables in the current database
\dt

-- Cleaned transactions (latest pipeline run)
SELECT COUNT(*), COUNT(DISTINCT invoice_no), MIN(invoice_date), MAX(invoice_date)
FROM retail_transactions
WHERE loaded_at = (SELECT MAX(loaded_at) FROM retail_transactions);

-- Top 10 products (latest pipeline run)
SELECT * FROM analysis_top10_products
WHERE loaded_at = (SELECT MAX(loaded_at) FROM analysis_top10_products)
ORDER BY quantity_sold DESC;

-- Monthly revenue trend (latest pipeline run)
SELECT * FROM analysis_monthly_revenue
WHERE loaded_at = (SELECT MAX(loaded_at) FROM analysis_monthly_revenue)
ORDER BY year_month;

-- SQL analysis results (latest pipeline run)
SELECT * FROM sql_top_3_products_last_6m
WHERE loaded_at = (SELECT MAX(loaded_at) FROM sql_top_3_products_last_6m)
ORDER BY month DESC, revenue_rank;

SELECT * FROM sql_rolling_3m_avg_australia
WHERE loaded_at = (SELECT MAX(loaded_at) FROM sql_rolling_3m_avg_australia)
ORDER BY month;
```

---

## 9 — Run SQL analysis queries manually

```bash
# Top 3 products by revenue per month (last 6 months)
docker compose exec postgres \
    psql -U retail -d retail \
    -f /opt/airflow/sql/top_3_products_last_6m.sql

# Rolling 3-month average revenue for Australia
docker compose exec postgres \
    psql -U retail -d retail \
    -f /opt/airflow/sql/rolling_3m_avg_australia.sql
```

Or copy-paste from [sql/top_3_products_last_6m.sql](sql/top_3_products_last_6m.sql) and [sql/rolling_3m_avg_australia.sql](sql/rolling_3m_avg_australia.sql) into any PostgreSQL client connected to the `retail` database.

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
============================== 65 passed in 10.xx s ==============================
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
    python -m pytest tests/unit/test_cleaning.py -v

# Single test class
docker compose --profile test run --rm tests \
    python -m pytest tests/unit/test_cleaning.py::TestCleanDataIntegration -v
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
4. Asserts all 5 tasks succeeded
5. Queries PostgreSQL to verify `retail_transactions`, `analysis_top10_products`, and `analysis_monthly_revenue`

Expected output ends with:

```
============================== 15 passed in XX.XXs ==============================
```

---

## Email notifications

The pipeline and test runner can send email alerts on failure. Notifications are **disabled by default** — set `ALERT_EMAIL` to a non-empty address to enable them.

### Where to configure

All SMTP settings live in **`docker-compose.yml`** under the `x-airflow-common` block (for DAG task alerts) and mirrored in the `tests` / `integration-tests` service blocks (for test-runner alerts):

```yaml
AIRFLOW__SMTP__SMTP_HOST: "smtp.example.com"   # ← your SMTP server
AIRFLOW__SMTP__SMTP_PORT: "587"
AIRFLOW__SMTP__SMTP_STARTTLS: "true"
AIRFLOW__SMTP__SMTP_SSL: "false"
AIRFLOW__SMTP__SMTP_USER: "sender@example.com" # ← sending address
AIRFLOW__SMTP__SMTP_PASSWORD: ""               # ← SMTP password or app password
AIRFLOW__SMTP__SMTP_MAIL_FROM: "sender@example.com"
ALERT_EMAIL: "alerts@example.com"             # ← recipient; set "" to disable
```

### What triggers a notification

| Event | Mechanism |
|-------|-----------|
| Any Airflow task fails (after all retries) | Airflow built-in `email_on_failure` — reads `AIRFLOW__SMTP__*` + `ALERT_EMAIL` |
| Any unit or integration test fails | `pytest_sessionfinish` hook in `tests/conftest.py` — reads the same env vars |

### Gmail example

1. Enable **2-Step Verification** on your Google account.
2. Generate an **App Password** (Google Account → Security → App Passwords).
3. In `docker-compose.yml` set:
   ```yaml
   AIRFLOW__SMTP__SMTP_HOST: "smtp.gmail.com"
   AIRFLOW__SMTP__SMTP_USER: "you@gmail.com"
   AIRFLOW__SMTP__SMTP_PASSWORD: "your-16-char-app-password"
   AIRFLOW__SMTP__SMTP_MAIL_FROM: "you@gmail.com"
   ALERT_EMAIL: "alerts@yourteam.com"
   ```
4. Restart the stack: `docker compose up -d airflow-webserver airflow-scheduler`

> **Security note:** avoid committing real SMTP credentials to version control.
> Use a `.env` file (listed in `.gitignore`) or a secrets manager in production.

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
                                         ┌─► run_pyspark_analysis
create_tables ──► ingest_and_clean ──────┤
                                         ├─► sql_top_3_products_last_6m
                                         └─► sql_rolling_3m_avg_australia
```

| Task | Tool | Description |
|------|------|-------------|
| `create_tables` | PythonOperator | Applies every DDL file in `sql/ddl/` (`CREATE TABLE IF NOT EXISTS`) so all output tables exist with precise column types before any data is written |
| `ingest_and_clean` | SparkSubmitOperator | Reads CSV, cleans data, anonymises CustomerID (PII), appends to `retail_transactions` |
| `run_pyspark_analysis` | SparkSubmitOperator | Reads from PostgreSQL; computes total revenue, top-10 products, monthly trend; appends to `analysis_top10_products` and `analysis_monthly_revenue` |
| `sql_top_3_products_last_6m` | PythonOperator | Executes `top_3_products_last_6m.sql`, logs sample rows, appends full result to `sql_top_3_products_last_6m` |
| `sql_rolling_3m_avg_australia` | PythonOperator | Executes `rolling_3m_avg_australia.sql`, logs sample rows, appends full result to `sql_rolling_3m_avg_australia` |

---

## Generated tables

All tables live in the `retail` PostgreSQL database and use **Type 2 append**: every pipeline run inserts new rows tagged with `loaded_at`. No data is ever overwritten or deleted. To query the latest snapshot for any table use:

```sql
WHERE loaded_at = (SELECT MAX(loaded_at) FROM <table>)
```

| Table | Written by | Columns |
|-------|------------|---------|
| `retail_transactions` | `ingest_and_clean` | `invoice_no`, `stock_code`, `description`, `quantity`, `invoice_date`, `unit_price`, `customer_id`, `country`, `revenue`, `is_cancellation`, **`loaded_at`** |
| `analysis_top10_products` | `run_pyspark_analysis` | `stock_code`, `quantity_sold`, **`loaded_at`** |
| `analysis_monthly_revenue` | `run_pyspark_analysis` | `year_month`, `monthly_revenue`, `num_transactions`, `num_customers`, `mom_growth_pct`, `yoy_growth_pct`, `rolling_3m_avg`, `rev_sigma_dist`, `mom_sigma_dist`, **`loaded_at`** |
| `sql_top_3_products_last_6m` | `sql_top_3_products_last_6m` | `month TEXT`, `stock_code TEXT`, `description TEXT`, `total_revenue_gbp NUMERIC`, `revenue_rank BIGINT`, **`loaded_at TIMESTAMP`** |
| `sql_rolling_3m_avg_australia` | `sql_rolling_3m_avg_australia` | `month TEXT`, `monthly_revenue_gbp NUMERIC`, `rolling_3m_avg_gbp NUMERIC`, **`loaded_at TIMESTAMP`** |

> **Column types** are defined explicitly in `sql/ddl/` — one `CREATE TABLE IF NOT EXISTS` file per table. The `create_tables` DAG task applies these files before any data is written, so every table is created with precise types (e.g. `BIGINT`, `NUMERIC`, `DOUBLE PRECISION`) rather than relying on Spark or cursor inference.

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
Edit `docker-compose.yml` to change any value.

### Pipeline

| Variable | Default | Description |
|----------|---------|-------------|
| `CSV_PATH` | `/opt/airflow/data/retails.csv` | Path to raw CSV inside containers |
| `POSTGRES_HOST` | `postgres` | PostgreSQL hostname |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `retail` | Retail database name |
| `POSTGRES_USER` | `retail` | Retail database user |
| `POSTGRES_PASSWORD` | `retail` | Retail database password |

### Email notifications

| Variable | Placeholder | Description |
|----------|-------------|-------------|
| `ALERT_EMAIL` | `alerts@example.com` | Failure alert recipient. Set to `""` to disable all notifications |
| `AIRFLOW__SMTP__SMTP_HOST` | `smtp.example.com` | SMTP server hostname |
| `AIRFLOW__SMTP__SMTP_PORT` | `587` | SMTP port (587 = STARTTLS, 465 = SSL) |
| `AIRFLOW__SMTP__SMTP_STARTTLS` | `true` | Use STARTTLS (`true`/`false`) |
| `AIRFLOW__SMTP__SMTP_SSL` | `false` | Use implicit SSL — set to `true` and port `465` for SSL-only servers |
| `AIRFLOW__SMTP__SMTP_USER` | `sender@example.com` | SMTP login username |
| `AIRFLOW__SMTP__SMTP_PASSWORD` | _(empty)_ | SMTP password or app-specific password |
| `AIRFLOW__SMTP__SMTP_MAIL_FROM` | `sender@example.com` | From address shown in alert emails |
