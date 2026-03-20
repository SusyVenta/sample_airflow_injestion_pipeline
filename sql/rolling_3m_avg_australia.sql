-- =============================================================================
-- rolling_3m_avg_australia.sql
-- Q2: Rolling 3-month average revenue for Australia
-- =============================================================================
-- Approach:
--   1. Build a complete month spine (all_months) covering the full dataset
--      date range using generate_series.  This ensures every calendar month
--      appears as a row, even if Australia had zero transactions that month.
--   2. LEFT JOIN the Australia monthly aggregates onto the spine.  Months with
--      no Australia sales get NULL monthly_revenue.
--   3. Apply ROWS BETWEEN 2 PRECEDING AND CURRENT ROW over the dense spine.
--      Because all months are now present, "2 rows back" always means exactly
--      2 calendar months back — no gaps can cause a row to skip over a month.
--   4. AVG() ignores NULLs, so a month with no sales is correctly excluded
--      from the average denominator (e.g. a 3-row window containing one NULL
--      averages over the 2 non-NULL months, not 3).
--
-- Why the month spine is anchored to the full dataset (not Australia-only):
--   Using the global date range guarantees the spine is consistent with other
--   queries.  Australia-only MIN/MAX would shift the spine if the country had
--   no early or late transactions, potentially hiding leading/trailing gaps.
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
),

all_months AS (
    -- Dense spine of every calendar month in the full dataset date range.
    SELECT generate_series(
               DATE_TRUNC('month', MIN(invoice_date))::DATE,
               DATE_TRUNC('month', MAX(invoice_date))::DATE,
               INTERVAL '1 month'
           )::DATE AS month
    FROM   retail_transactions
)

SELECT
    TO_CHAR(a.month, 'YYYY-MM')                        AS month,
    ROUND(m.monthly_revenue::NUMERIC, 2)               AS monthly_revenue_gbp,
    ROUND(
        AVG(m.monthly_revenue) OVER (
            ORDER BY a.month
            -- Dense spine makes ROWS == calendar months; NULLs are skipped by AVG
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::NUMERIC,
        2
    )                                                  AS rolling_3m_avg_gbp
FROM   all_months a
LEFT   JOIN monthly_australia m USING (month)
ORDER  BY a.month;
