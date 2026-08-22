SELECT
  order_date,
  SUM(total_orders) AS total_orders,
  ROUND(SUM(total_revenue) / NULLIF(SUM(total_orders), 0), 2) AS avg_order_value
FROM zaferan_sofreh.gold_v2.daily_sales_summary
WHERE ( :date_range.min IS NULL OR order_date >= DATE(:date_range.min) )
  AND ( :date_range.max IS NULL OR order_date <= DATE(:date_range.max) )
GROUP BY order_date
ORDER BY order_date ASC;