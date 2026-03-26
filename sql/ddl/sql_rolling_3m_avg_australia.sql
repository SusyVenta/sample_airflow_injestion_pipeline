-- =============================================================================
-- ddl/sql_rolling_3m_avg_australia.sql
-- Output of: sql_rolling_3m_avg_australia (PythonOperator → rolling_3m_avg_australia.sql)
--
-- Column types match the PostgreSQL query output from rolling_3m_avg_australia.sql:
--   month               TO_CHAR(…, 'YYYY-MM')                  → TEXT
--   monthly_revenue_gbp ROUND(monthly_revenue::NUMERIC, 2)     → NUMERIC  (nullable: LEFT JOIN months
--                       with no Australia sales produce NULL)
--   rolling_3m_avg_gbp  ROUND(AVG(…)::NUMERIC, 2) OVER (…)    → NUMERIC  (nullable: AVG over all-NULL
--                       windows returns NULL)
--   loaded_at           Python datetime.now(utc)               → TIMESTAMP
-- =============================================================================

CREATE TABLE IF NOT EXISTS sql_rolling_3m_avg_australia (
    month               TEXT      NOT NULL,
    monthly_revenue_gbp NUMERIC,              -- NULL for months with no Australia sales
    rolling_3m_avg_gbp  NUMERIC,              -- NULL when all window rows are NULL
    loaded_at           TIMESTAMP NOT NULL
);
