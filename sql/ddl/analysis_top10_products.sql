-- =============================================================================
-- ddl/analysis_top10_products.sql
-- Output of: run_pyspark_analysis (SparkSubmitOperator → analysis.py)
--            get_top_10_products() → save_to_postgres()
--
-- stock_code    StringType → TEXT
-- quantity_sold DoubleType  → DOUBLE PRECISION  (sum of quantity column)
-- loaded_at     TimestampType → TIMESTAMP       (appended by save_to_postgres)
-- =============================================================================

CREATE TABLE IF NOT EXISTS analysis_top10_products (
    stock_code    TEXT              NOT NULL,
    quantity_sold DOUBLE PRECISION  NOT NULL,
    loaded_at     TIMESTAMP         NOT NULL
);
