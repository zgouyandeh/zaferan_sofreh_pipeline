SELECT
  ROUND(s.restaurant_health_score, 4) AS restaurant_health_score,
  ROUND(s.repeat_customer_rate * 100, 2) AS repeat_customer_rate_pct,
  s.avg_order_value,
  s.total_orders,
  s.total_revenue,
  s.restaurant_id,
  COALESCE(r.name, s.restaurant_id) AS restaurant_name
FROM zaferan_sofreh.gold_v2.restaurant_360 s
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r
       ON s.restaurant_id = r.restaurant_id
ORDER BY s.restaurant_health_score DESC;