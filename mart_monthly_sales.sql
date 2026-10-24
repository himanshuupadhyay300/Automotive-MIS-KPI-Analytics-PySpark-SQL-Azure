-- Monthly sales trend: revenue, margin, month-over-month growth, fiscal YTD and a rolling 3-month average.
WITH monthly AS (
    SELECT d.year_month,
           d.year,
           d.fiscal_year,
           SUM(f.quantity)                    AS units_sold,
           COUNT(*)                           AS transactions,
           ROUND(SUM(f.net_amount), 2)        AS net_revenue,
           ROUND(SUM(f.gross_profit), 2)      AS gross_profit,
           ROUND(AVG(f.discount_pct), 2)      AS avg_discount_pct
    FROM fact_sales f
    JOIN dim_date d ON d.date_key = f.date_key
    GROUP BY d.year_month, d.year, d.fiscal_year
)
SELECT year_month,
       year,
       fiscal_year,
       units_sold,
       transactions,
       net_revenue,
       gross_profit,
       avg_discount_pct,
       ROUND(100.0 * gross_profit / net_revenue, 2) AS gross_margin_pct,
       ROUND(100.0 * (net_revenue - LAG(net_revenue) OVER (ORDER BY year_month))
             / LAG(net_revenue) OVER (ORDER BY year_month), 2) AS mom_revenue_growth_pct,
       ROUND(SUM(net_revenue) OVER (PARTITION BY fiscal_year ORDER BY year_month), 2) AS fiscal_ytd_revenue,
       ROUND(AVG(net_revenue) OVER (ORDER BY year_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rolling_3m_avg_revenue
FROM monthly
