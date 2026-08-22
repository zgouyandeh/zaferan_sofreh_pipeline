SELECT
  f.predicted_revenue / 1e6 AS predicted_revenue_m,
  f.restaurant_id,
  COALESCE(r.name, f.restaurant_id) AS restaurant_name
FROM zaferan_sofreh.platinum.daily_revenue_forecast_by_restaurant f
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r
       ON f.restaurant_id = r.restaurant_id;