-- Ad-hoc analytical queries over the star schema and the KPI marts.
-- Run: python -m src.utils.sql_runner sql/analysis_queries.sql

-- name: Q1 Sales trend by month (mart)
SELECT year_month, units_sold, net_revenue, gross_margin_pct, mom_revenue_growth_pct, rolling_3m_avg_revenue
FROM mart_monthly_sales
ORDER BY year_month;

-- name: Q2 Revenue by region and segment (star schema join)
SELECT dd.region, p.segment,
       SUM(f.quantity)               AS units,
       ROUND(SUM(f.net_amount), 0)   AS net_revenue,
       ROUND(100.0 * SUM(f.gross_profit) / SUM(f.net_amount), 2) AS margin_pct
FROM fact_sales f
JOIN dim_dealership dd ON dd.dealership_key = f.dealership_key
JOIN dim_product p     ON p.product_key = f.product_key
GROUP BY dd.region, p.segment
ORDER BY dd.region, net_revenue DESC;

-- name: Q3 Dealership leaderboard for the latest month
SELECT revenue_rank, dealership_name, region, net_revenue, target_attainment_pct, gross_margin_pct
FROM mart_dealership_monthly
WHERE year_month = (SELECT MAX(year_month) FROM mart_dealership_monthly)
ORDER BY revenue_rank;

-- name: Q4 Target attainment: months a dealership met or beat its target
SELECT dealership_name,
       SUM(CASE WHEN target_attainment_pct >= 100 THEN 1 ELSE 0 END) AS months_on_target,
       COUNT(*)                                                      AS months_reported,
       ROUND(AVG(target_attainment_pct), 1)                          AS avg_attainment_pct
FROM mart_dealership_monthly
GROUP BY dealership_name
ORDER BY avg_attainment_pct DESC;

-- name: Q5 Product ABC classification and turnover
SELECT abc_class, model, variant, segment, units_sold, net_revenue, revenue_share_pct, inventory_turnover_for_period
FROM mart_product_kpis
ORDER BY net_revenue DESC;

-- name: Q6 Discounting vs margin by payment type
SELECT payment_type,
       COUNT(*)                         AS transactions,
       ROUND(AVG(discount_pct), 2)      AS avg_discount_pct,
       ROUND(100.0 * SUM(gross_profit) / SUM(net_amount), 2) AS margin_pct
FROM fact_sales
GROUP BY payment_type
ORDER BY margin_pct DESC;

-- name: Q7 Inventory movement: thinnest stock cover (lowest weeks of cover) and stock-out weeks
SELECT week_end, dealership_id, model, closing_units, units_sold, weeks_of_cover, is_stockout
FROM mart_inventory_weekly
WHERE weeks_of_cover IS NOT NULL
ORDER BY weeks_of_cover, week_end, dealership_id
LIMIT 10;

-- name: Q8 Inventory reconciliation exceptions (opening + received - sold - closing <> 0)
SELECT week_end, dealership_id, product_id, opening_units, units_received, units_sold, closing_units, reconciliation_diff
FROM mart_inventory_weekly
WHERE reconciliation_diff <> 0
ORDER BY week_end, dealership_id, product_id;

-- name: Q9 Data quality: rejected sales by rule
SELECT rule, COUNT(*) AS rejected_rows
FROM (SELECT explode(_dq_errors) AS rule FROM rejects_sales)
GROUP BY rule
ORDER BY rejected_rows DESC;

-- name: Q10 Late-arriving sales by month
SELECT year_month, SUM(CASE WHEN is_late_arrival THEN 1 ELSE 0 END) AS late_sales, COUNT(*) AS sales
FROM fact_sales
GROUP BY year_month
ORDER BY year_month;
