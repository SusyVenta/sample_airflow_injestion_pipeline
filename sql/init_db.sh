#!/usr/bin/env bash
# =============================================================================
# init_db.sh
# Runs once on first PostgreSQL container start (mounted into
# /docker-entrypoint-initdb.d/).  Creates the `airflow` and `retail`
# databases with dedicated users.
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
