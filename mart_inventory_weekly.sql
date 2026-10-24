-- Weekly inventory movement per dealership x product, with a reconciliation check:
--   opening + received - sold - closing  must equal 0
-- A non-zero difference means sales (or snapshots) are missing or wrong, e.g. late-arriving sales.
-- Week = Monday..Sunday; snapshots are taken at the end of Sunday.
WITH weekly_sales AS (
    SELECT dealership_key, product_key,
           date_add(CAST(date_trunc('week', sale_date) AS DATE), 6) AS week_end,
           SUM(quantity) AS units_sold
    FROM fact_sales
    GROUP BY dealership_key, product_key, date_add(CAST(date_trunc('week', sale_date) AS DATE), 6)
),
snap AS (
    SELECT s.snapshot_date AS week_end,
           s.dealership_key, s.product_key,
           s.units_on_hand AS closing_units,
           s.units_received,
           s.stock_value_at_cost,
           LAG(s.units_on_hand) OVER (PARTITION BY s.dealership_key, s.product_key ORDER BY s.snapshot_date) AS opening_units
    FROM fact_inventory_snapshot s
),
moved AS (
    SELECT n.*, COALESCE(w.units_sold, 0) AS units_sold
    FROM snap n
    LEFT JOIN weekly_sales w
           ON w.dealership_key = n.dealership_key AND w.product_key = n.product_key AND w.week_end = n.week_end
)
SELECT m.week_end,
       dd.dealership_id,
       p.product_id,
       p.model,
       m.opening_units,
       m.units_received,
       m.units_sold,
       m.closing_units,
       m.opening_units + m.units_received - m.units_sold - m.closing_units AS reconciliation_diff,
       m.stock_value_at_cost,
       ROUND(m.closing_units / NULLIF(AVG(m.units_sold) OVER (PARTITION BY m.dealership_key, m.product_key
             ORDER BY m.week_end ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 0), 1) AS weeks_of_cover,
       (m.closing_units = 0) AS is_stockout
FROM moved m
JOIN dim_dealership dd ON dd.dealership_key = m.dealership_key
JOIN dim_product p     ON p.product_key = m.product_key
