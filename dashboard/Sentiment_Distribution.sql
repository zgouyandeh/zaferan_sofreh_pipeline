SELECT
  COALESCE(r.name, s.restaurant_id) AS restaurant_name,
  'Positive' AS sentiment_category,
  SUM(s.sentiment_positive_review) AS review_count
FROM zaferan_sofreh.gold_v2.daily_restaurant_reviews s
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r 
       ON s.restaurant_id = r.restaurant_id
GROUP BY r.name, s.restaurant_id

UNION ALL

SELECT
  COALESCE(r.name, s.restaurant_id) AS restaurant_name,
  'Neutral' AS sentiment_category,
  SUM(s.sentiment_neutral_review) AS review_count
FROM zaferan_sofreh.gold_v2.daily_restaurant_reviews s
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r 
       ON s.restaurant_id = r.restaurant_id
GROUP BY r.name, s.restaurant_id

UNION ALL

SELECT
  COALESCE(r.name, s.restaurant_id) AS restaurant_name,
  'Negative' AS sentiment_category,
  SUM(s.sentiment_negative_review) AS review_count
FROM zaferan_sofreh.gold_v2.daily_restaurant_reviews s
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r 
       ON s.restaurant_id = r.restaurant_id
GROUP BY r.name, s.restaurant_id;