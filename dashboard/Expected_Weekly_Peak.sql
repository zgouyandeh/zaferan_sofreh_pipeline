SELECT
  f.forecast_date,
  date_format(f.forecast_date, 'EEEE') AS peak_day_name,
  CONCAT(
    ROUND(SUM(f.predicted_revenue) / 1e6, 1), '(Upper: ',
    ROUND(SUM(f.upper_ci) / 1e6, 1), ')'
  ) AS display_value,
  f.restaurant_id,
  COALESCE(r.name, f.restaurant_id) AS restaurant_name
FROM zaferan_sofreh.platinum.daily_revenue_forecast_by_restaurant f
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r
       ON f.restaurant_id = r.restaurant_id
GROUP BY f.forecast_date, f.restaurant_id, r.name
ORDER BY SUM(f.predicted_revenue) DESC;
 