SELECT
  TRIM(REGEXP_REPLACE(COALESCE(r.name, s.restaurant_id), '(?i)zaferan\\s*sofreh\\s*[-–—]?\\s*', '')) AS restaurant_name,
  'Food Quality' AS issue_dimension,
  s.food_quality_complaint_count AS complaint_count,
  COALESCE(s.avg_food_quality_severity, 0.0) AS avg_severity
FROM zaferan_sofreh.gold_v2.restaurant_360 s
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r ON s.restaurant_id = r.restaurant_id

UNION ALL

SELECT
  TRIM(REGEXP_REPLACE(COALESCE(r.name, s.restaurant_id), '(?i)zaferan\\s*sofreh\\s*[-–—]?\\s*', '')) AS restaurant_name,
  'Pricing' AS issue_dimension,
  s.pricing_complaint_count AS complaint_count,
  COALESCE(s.avg_pricing_severity, 0.0) AS avg_severity
FROM zaferan_sofreh.gold_v2.restaurant_360 s
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r ON s.restaurant_id = r.restaurant_id

UNION ALL

SELECT
  TRIM(REGEXP_REPLACE(COALESCE(r.name, s.restaurant_id), '(?i)zaferan\\s*sofreh\\s*[-–—]?\\s*', '')) AS restaurant_name,
  'Portion Size' AS issue_dimension,
  s.portion_complaint_count AS complaint_count,
  COALESCE(s.avg_portion_severity, 0.0) AS avg_severity
FROM zaferan_sofreh.gold_v2.restaurant_360 s
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r ON s.restaurant_id = r.restaurant_id

UNION ALL

SELECT
  TRIM(REGEXP_REPLACE(COALESCE(r.name, s.restaurant_id), '(?i)zaferan\\s*sofreh\\s*[-–—]?\\s*', '')) AS restaurant_name,
  'Delivery' AS issue_dimension,
  s.delivery_complaint_count AS complaint_count,
  COALESCE(s.avg_delivery_severity, 0.0) AS avg_severity
FROM zaferan_sofreh.gold_v2.restaurant_360 s
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r ON s.restaurant_id = r.restaurant_id;