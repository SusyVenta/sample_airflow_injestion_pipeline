-- =============================================================================
-- analysis.sql
-- Online Retail – SQL-based Data Analysis
-- =============================================================================
-- Two queries are defined here:
--   Q1. Top 3 products by revenue for each month over the last 6 months
--   Q2. Rolling 3-month average revenue for Australia
--
-- Both queries run against the `retail_transactions` table that is created and
-- populated by the PySpark ingestion job (clean_and_ingest.py).
--
-- "Last 6 months" is computed relative to the most recent invoice date in the
-- dataset (not CURRENT_DATE) so that the queries return results regardless of
-- when the pipeline is executed against the historical dataset.
-- Replace the sub-select with `CURRENT_DATE` in a live production context.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Q1: Top 3 products by revenue per month  (last 6 months of dataset data)
-- ---------------------------------------------------------------------------
-- Approach:
--   1. Filter to the last 6 months and valid (non-cancelled, positive) rows.
--   2. Aggregate revenue by (month, product).
--   3. Rank products within each month by descending revenue using RANK().
--      RANK() is chosen over ROW_NUMBER() so that tied products at position 3
--      are both included rather than arbitrarily cut off.
--   4. Keep only rank <= 3.
-- ---------------------------------------------------------------------------

WITH dataset_max_date AS (
    -- Anchor "today" to the dataset's most recent invoice to keep results
    -- reproducible when running against the historical CSV.
    SELECT MAX(invoice_date) AS max_dt
    FROM   retail_transactions
),

last_6_months_window AS (
    SELECT max_dt,
           max_dt - INTERVAL '6 months' AS window_start
    FROM   dataset_max_date
),

monthly_product_revenue AS (
    -- Aggregate first; window function applied in the next CTE to avoid
    -- referencing the raw column inside PARTITION BY in an aggregated query.
    SELECT
        DATE_TRUNC('month', t.invoice_date)::DATE  AS month,
        t.stock_code,
        t.description,
        SUM(t.revenue)                             AS total_revenue
    FROM   retail_transactions t
    CROSS  JOIN last_6_months_window w
    WHERE  t.invoice_date  >  w.window_start      -- strictly after window start
      AND  t.invoice_date  <= w.max_dt
      AND  t.is_cancellation = FALSE
      AND  t.revenue         > 0
    GROUP  BY 1, 2, 3
),

ranked AS (
    -- Apply RANK after aggregation: month is now a plain column so
    -- PARTITION BY / ORDER BY have no GROUP BY conflict.
    -- RANK allows ties: if two products share revenue at rank 3, both appear.
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
    TO_CHAR(month, 'YYYY-MM')              AS month,
    stock_code,
    description,
    ROUND(total_revenue::NUMERIC, 2)       AS total_revenue_gbp,
    revenue_rank
FROM   ranked
WHERE  revenue_rank <= 3
ORDER  BY month DESC, revenue_rank;


-- ---------------------------------------------------------------------------
-- Q2: Rolling 3-month average revenue for Australia
-- ---------------------------------------------------------------------------
-- Approach:
--   1. Aggregate monthly revenue for Australia (non-cancelled, positive rows).
--   2. Apply a sliding window of 3 rows (current month + 2 preceding months)
--      ordered chronologically.
--   3. The average therefore covers month N-2, N-1, and N.
--      For the first two months of the series the window contains fewer rows,
--      so the average is over 1 and 2 months respectively — this is the
--      standard behaviour of a "expanding then fixed" rolling average.
-- ---------------------------------------------------------------------------

WITH monthly_australia AS (
    SELECT
        DATE_TRUNC('month', invoice_date)::DATE  AS month,
        SUM(revenue)                             AS monthly_revenue
    FROM   retail_transactions
    WHERE  country          = 'Australia'
      AND  is_cancellation  = FALSE
      AND  revenue          > 0
    GROUP  BY 1
)

SELECT
    TO_CHAR(month, 'YYYY-MM')                          AS month,
    ROUND(monthly_revenue::NUMERIC, 2)                 AS monthly_revenue_gbp,
    ROUND(
        AVG(monthly_revenue) OVER (
            ORDER BY month
            -- 3-month window: current row + 2 preceding months
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::NUMERIC,
        2
    )                                                  AS rolling_3m_avg_gbp
FROM   monthly_australia
ORDER  BY month;
