-- =============================================================================
-- top_3_products_last_6m.sql
-- Q1: Top 3 products by revenue per month (last 6 months of dataset data)
-- =============================================================================
-- Approach:
--   1. Compute MAX(invoice_date) once in dataset_max_date; reference it as a
--      scalar subquery in WHERE to filter without a join.
--      "Last 6 months" is anchored to the dataset's most recent invoice date
--      (not CURRENT_DATE) so the query is reproducible against the historical CSV.
--   2. Aggregate revenue by (month, product) in monthly_product_revenue —
--      necessary because PostgreSQL does not allow a window function and GROUP BY
--      in the same SELECT (RANK must reference the already-aggregated total_revenue).
--   3. Apply DENSE_RANK() in the outer query, then filter to rank <= 3.
--      • vs ROW_NUMBER(): arbitrarily breaks ties — two equally-ranked
--        products at position 3 would not both appear.
--      • vs RANK(): skips ranks after a tie, so two products tied at position 2
--        would make the next product rank 4, silently dropping what should be
--        the 3rd-place result.
--      DENSE_RANK() is correct here: ties share the same rank and the next
--      rank is not skipped, so the 3rd slot is always filled even when
--      positions 1 or 2 have ties.  All products that tie *at* rank 3 are
--      also included.
-- =============================================================================

WITH dataset_max_date AS (
    -- Truncate to month so the 6-month window aligns on calendar-month
    -- boundaries.  Without truncation MAX(invoice_date) is a mid-month
    -- timestamp, and subtracting 6 months would cut off the partial first
    -- month.  Note: QUALIFY (BigQuery/Snowflake/DuckDB) is not available in
    -- PostgreSQL, so the revenue_rank filter is applied via a subquery below.
    SELECT DATE_TRUNC('month', MAX(invoice_date))::DATE AS max_month
    FROM   retail_transactions
),

monthly_product_revenue AS (
    SELECT
        DATE_TRUNC('month', invoice_date)::DATE  AS month,
        stock_code,
        description,
        SUM(revenue)                             AS total_revenue
    FROM   retail_transactions
    WHERE  DATE_TRUNC('month', invoice_date) BETWEEN
               (SELECT max_month - INTERVAL '6 months' FROM dataset_max_date)
           AND (SELECT max_month                        FROM dataset_max_date)
      AND  is_cancellation = FALSE
      AND  revenue         > 0
      AND  stock_code      != 'UNKNOWN'
    GROUP  BY 1, 2, 3
)

SELECT
    TO_CHAR(month, 'YYYY-MM')              AS month,
    stock_code,
    description,
    ROUND(total_revenue::NUMERIC, 2)       AS total_revenue_gbp,
    revenue_rank
FROM (
    SELECT
        month,
        stock_code,
        description,
        total_revenue,
        DENSE_RANK() OVER (
            PARTITION BY month
            ORDER BY     total_revenue DESC
        ) AS revenue_rank
    FROM  monthly_product_revenue
) ranked
WHERE  revenue_rank <= 3
ORDER  BY month DESC, revenue_rank;
