#!/usr/bin/env bash
# =============================================================================
# init_db.sh
# Runs once on first PostgreSQL container start (mounted into
# /docker-entrypoint-initdb.d/).  Creates two logically separate databases
# on the shared PostgreSQL instance:
#
#   airflow  – Airflow's internal metadata store: DAG runs, task instances,
#              connections, variables, user accounts, scheduler heartbeats.
#              Owned by the `airflow` user; the pipeline has no access to it.
#
#   retail   – The pipeline's application database: retail_transactions and
#              all analysis output tables written by the Spark jobs and SQL
#              tasks.  Owned by the `retail` user; Airflow itself never writes
#              here (only the tasks it orchestrates do).
#
# Keeping the two databases separate enforces least-privilege isolation:
# the `retail` user cannot read Airflow internals, and the `airflow` user
# cannot access business data.  In production each would typically run on its
# own database cluster; sharing a single Postgres instance here is a
# development/demo convenience to keep the Docker Compose stack minimal.
# =============================================================================
set -euo pipefail

# The entrypoint already connected as POSTGRES_USER (postgres superuser).
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    -- ------------------------------------------------------------------
    -- Airflow metadata database
    -- ------------------------------------------------------------------
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow') THEN
            CREATE USER airflow WITH PASSWORD 'airflow';
        END IF;
    END
    \$\$;

    SELECT 'CREATE DATABASE airflow OWNER airflow'
    WHERE  NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')
    \gexec

    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

    -- ------------------------------------------------------------------
    -- Retail pipeline database
    -- ------------------------------------------------------------------
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'retail') THEN
            CREATE USER retail WITH PASSWORD 'retail';
        END IF;
    END
    \$\$;

    SELECT 'CREATE DATABASE retail OWNER retail'
    WHERE  NOT EXISTS (SELECT FROM pg_database WHERE datname = 'retail')
    \gexec

    GRANT ALL PRIVILEGES ON DATABASE retail TO retail;

EOSQL

echo "[init_db] Databases 'airflow' and 'retail' ready."
