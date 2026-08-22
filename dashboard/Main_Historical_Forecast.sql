SELECT
    p.activity_date AS date,
    p.daily_revenue / 1e6 AS amount,
    'Actual' AS metric_type,
    CAST(NULL AS DOUBLE) AS lower_ci,
    CAST(NULL AS DOUBLE) AS upper_ci,
    p.restaurant_id,
    COALESCE(r.name, p.restaurant_id) AS restaurant_name
FROM zaferan_sofreh.gold_v2.daily_restaurant_performance p
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r
       ON p.restaurant_id = r.restaurant_id
WHERE p.activity_date >= CURRENT_DATE() - INTERVAL 14 DAYS
 
UNION ALL
 
SELECT
    f.forecast_date AS date,
    f.predicted_revenue / 1e6 AS amount,
    'Forecast' AS metric_type,
    f.lower_ci / 1e6 AS lower_ci,
    f.upper_ci / 1e6 AS upper_ci,
    f.restaurant_id,
    COALESCE(r.name, f.restaurant_id) AS restaurant_name
FROM zaferan_sofreh.platinum.daily_revenue_forecast_by_restaurant f
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r
       ON f.restaurant_id = r.restaurant_id
 
ORDER BY date ASC;
 
 