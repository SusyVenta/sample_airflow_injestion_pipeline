-- =============================================================================
-- rolling_3m_avg_australia.sql
-- Q2: Rolling 3-month average revenue for Australia
-- =============================================================================
-- Approach:
--   1. Aggregate monthly revenue for Australia (non-cancelled, positive rows).
--   2. Apply a sliding window of 3 rows (current month + 2 preceding months)
--      ordered chronologically.
--   3. The average therefore covers month N-2, N-1, and N.
--      For the first two months of the series the window contains fewer rows,
--      so the average is over 1 and 2 months respectively — this is the
--      standard behaviour of a "expanding then fixed" rolling average.
-- =============================================================================

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
