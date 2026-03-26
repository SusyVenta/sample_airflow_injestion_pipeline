-- =============================================================================
-- ddl/sql_top_3_products_last_6m.sql
-- Output of: sql_top_3_products_last_6m (PythonOperator → top_3_products_last_6m.sql)
--
-- Column types match the PostgreSQL query output from top_3_products_last_6m.sql:
--   month             TO_CHAR(…, 'YYYY-MM')              → TEXT
--   stock_code        TEXT column from retail_transactions → TEXT
--   description       TEXT column from retail_transactions → TEXT    (nullable)
--   total_revenue_gbp ROUND(total_revenue::NUMERIC, 2)   → NUMERIC
--   revenue_rank      DENSE_RANK() window function        → BIGINT
--   loaded_at         Python datetime.now(utc)            → TIMESTAMP
-- =============================================================================

CREATE TABLE IF NOT EXISTS sql_top_3_products_last_6m (
    month             TEXT      NOT NULL,
    stock_code        TEXT      NOT NULL,
    description       TEXT,
    total_revenue_gbp NUMERIC   NOT NULL,
    revenue_rank      BIGINT    NOT NULL,
    loaded_at         TIMESTAMP NOT NULL
);
