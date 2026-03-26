-- =============================================================================
-- ddl/analysis_monthly_revenue.sql
-- Output of: run_pyspark_analysis (SparkSubmitOperator → analysis.py)
--            get_monthly_revenue_trend() → save_to_postgres()
--
-- Column type mapping (PySpark → PostgreSQL JDBC):
--   year_month       date_format() → StringType  → TEXT
--   monthly_revenue  F.round(sum)  → DoubleType  → DOUBLE PRECISION
--   num_transactions countDistinct → LongType    → BIGINT
--   num_customers    countDistinct → LongType    → BIGINT
--   mom_growth_pct   F.round(lag)  → DoubleType  → DOUBLE PRECISION  (NULL for first month)
--   yoy_growth_pct   F.round(lag)  → DoubleType  → DOUBLE PRECISION  (NULL when < 12 prior months)
--   rolling_3m_avg   F.round(avg)  → DoubleType  → DOUBLE PRECISION
--   rev_sigma_dist   F.round(std)  → DoubleType  → DOUBLE PRECISION  (NULL when std undefined)
--   mom_sigma_dist   F.round(std)  → DoubleType  → DOUBLE PRECISION  (NULL when std undefined)
--   loaded_at        current_timestamp → TimestampType → TIMESTAMP
-- =============================================================================

CREATE TABLE IF NOT EXISTS analysis_monthly_revenue (
    year_month        TEXT              NOT NULL,
    monthly_revenue   DOUBLE PRECISION,
    num_transactions  BIGINT,
    num_customers     BIGINT,
    mom_growth_pct    DOUBLE PRECISION,            -- NULL for the first month in the dataset
    yoy_growth_pct    DOUBLE PRECISION,            -- NULL when fewer than 12 prior months exist
    rolling_3m_avg    DOUBLE PRECISION,
    rev_sigma_dist    DOUBLE PRECISION,            -- NULL when rolling std is undefined (n < 2)
    mom_sigma_dist    DOUBLE PRECISION,            -- NULL when rolling std is undefined (n < 2)
    loaded_at         TIMESTAMP         NOT NULL
);
