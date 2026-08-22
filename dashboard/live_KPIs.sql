WITH daily_metrics AS (
  SELECT
    COALESCE(SUM(total_orders), 0) AS total_orders,
    COALESCE(SUM(total_revenue), 0) AS total_revenue,
    COALESCE(SUM(total_revenue) / NULLIF(SUM(total_orders), 0), 0) AS avg_order_value,
    COALESCE(hll_sketch_estimate(hll_union_agg(customer_hll_sketch)), 0) AS unique_customers
  FROM zaferan_sofreh.gold_v2.daily_sales_summary
  WHERE ( :date_range.min IS NULL OR order_date >= :date_range.min )
    AND ( :date_range.max IS NULL OR order_date <= :date_range.max )
),
customer_metrics AS (
  SELECT
    COALESCE(COUNT_IF(customer_activity_status = 'active'), 0) AS active_customers
  FROM zaferan_sofreh.gold_v2.customer_360
  WHERE ( :date_range.min IS NULL OR last_order_date >= :date_range.min )
    AND ( :date_range.max IS NULL OR last_order_date <= :date_range.max )
)
SELECT 
  dm.total_orders,
  dm.total_revenue,
  dm.avg_order_value,
  dm.unique_customers,
  cm.active_customers
FROM daily_metrics dm
CROSS JOIN customer_metrics cm;