-- Dealership performance per month: revenue, margin, target attainment, rank and share of the month.
-- Note: dim_dealership is SCD Type 1, so the CURRENT target is applied to all historical months.
WITH m AS (
    SELECT d.year_month,
           f.dealership_key,
           SUM(f.quantity)                AS units_sold,
           ROUND(SUM(f.net_amount), 2)    AS net_revenue,
           ROUND(SUM(f.gross_profit), 2)  AS gross_profit,
           ROUND(AVG(f.discount_pct), 2)  AS avg_discount_pct
    FROM fact_sales f
    JOIN dim_date d ON d.date_key = f.date_key
    GROUP BY d.year_month, f.dealership_key
)
SELECT m.year_month,
       dd.dealership_id,
       dd.dealership_name,
       dd.region,
       dd.city,
       m.units_sold,
       m.net_revenue,
       m.gross_profit,
       ROUND(100.0 * m.gross_profit / m.net_revenue, 2)                          AS gross_margin_pct,
       m.avg_discount_pct,
       dd.monthly_sales_target,
       ROUND(100.0 * m.net_revenue / dd.monthly_sales_target, 2)                 AS target_attainment_pct,
       RANK() OVER (PARTITION BY m.year_month ORDER BY m.net_revenue DESC)       AS revenue_rank,
       ROUND(100.0 * m.net_revenue / SUM(m.net_revenue) OVER (PARTITION BY m.year_month), 2) AS revenue_share_pct
FROM m
JOIN dim_dealership dd ON dd.dealership_key = m.dealership_key
