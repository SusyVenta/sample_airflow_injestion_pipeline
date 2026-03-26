-- =============================================================================
-- ddl/retail_transactions.sql
-- Output of: ingest_and_clean (SparkSubmitOperator → clean_and_ingest.py)
--
-- Column types mirror the PySpark DataFrame schema produced by clean_data():
--   StringType   → TEXT
--   DoubleType   → DOUBLE PRECISION
--   TimestampType→ TIMESTAMP
--   BooleanType  → BOOLEAN
--
-- loaded_at is appended by load_to_postgres() before every JDBC write.
-- Each pipeline run inserts a new batch; query the latest snapshot with:
--   WHERE loaded_at = (SELECT MAX(loaded_at) FROM retail_transactions)
-- =============================================================================

CREATE TABLE IF NOT EXISTS retail_transactions (
    invoice_no      TEXT              NOT NULL,
    stock_code      TEXT              NOT NULL,
    description     TEXT,
    quantity        DOUBLE PRECISION  NOT NULL,
    invoice_date    TIMESTAMP         NOT NULL,
    unit_price      DOUBLE PRECISION  NOT NULL,
    customer_id     TEXT              NOT NULL,  -- SHA-256 hash or 'ANONYMOUS'
    country         TEXT              NOT NULL,
    revenue         DOUBLE PRECISION  NOT NULL,
    is_cancellation BOOLEAN           NOT NULL,
    loaded_at       TIMESTAMP         NOT NULL
);
