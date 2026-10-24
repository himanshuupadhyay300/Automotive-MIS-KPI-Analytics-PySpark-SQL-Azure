-- Product KPIs over the whole period: sales, margin, discounting, ABC class by revenue, inventory turnover.
-- ABC: a product is class A if the cumulative revenue share BEFORE it is below the A threshold, B below the
-- B threshold, otherwise C (thresholds come from config: gold.abc_a_threshold / abc_b_threshold).
WITH sales AS (
    SELECT product_key,
           SUM(quantity)       AS units_sold,
           SUM(net_amount)     AS net_revenue,
           SUM(gross_profit)   AS gross_profit,
           AVG(discount_pct)   AS avg_discount_pct
    FROM fact_sales
    GROUP BY product_key
),
stock AS (
    SELECT product_key, AVG(total_on_hand) AS avg_units_on_hand
    FROM (SELECT product_key, snapshot_date, SUM(units_on_hand) AS total_on_hand
          FROM fact_inventory_snapshot GROUP BY product_key, snapshot_date)
    GROUP BY product_key
),
joined AS (
    SELECT p.product_id, p.model, p.variant, p.segment, p.fuel_type,
           s.units_sold,
           ROUND(s.net_revenue, 2)       AS net_revenue,
           ROUND(s.gross_profit, 2)      AS gross_profit,
           ROUND(s.avg_discount_pct, 2)  AS avg_discount_pct,
           ROUND(k.avg_units_on_hand, 1) AS avg_units_on_hand
    FROM sales s
    JOIN dim_product p ON p.product_key = s.product_key
    LEFT JOIN stock k  ON k.product_key = s.product_key
),
ranked AS (
    SELECT *,
           SUM(net_revenue) OVER (ORDER BY net_revenue DESC, product_id
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(net_revenue) OVER () AS cum_share,
           net_revenue / SUM(net_revenue) OVER ()                                                             AS own_share
    FROM joined
)
SELECT product_id, model, variant, segment, fuel_type,
       units_sold, net_revenue, gross_profit,
       ROUND(100.0 * gross_profit / net_revenue, 2)  AS gross_margin_pct,
       avg_discount_pct,
       ROUND(100.0 * own_share, 2)                    AS revenue_share_pct,
       ROUND(100.0 * cum_share, 2)                    AS cumulative_revenue_share_pct,
       CASE WHEN cum_share - own_share < ${abc_a} THEN 'A'
            WHEN cum_share - own_share < ${abc_b} THEN 'B'
            ELSE 'C' END                              AS abc_class,
       avg_units_on_hand,
       ROUND(units_sold / NULLIF(avg_units_on_hand, 0), 2) AS inventory_turnover_for_period
FROM ranked
