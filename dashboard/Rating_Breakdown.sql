SELECT
  COALESCE(r.name, s.restaurant_id) AS restaurant_name,
  '5 Stars' AS rating_label, 5 AS star_rank, SUM(s.rating_5_stars) AS rating_count
FROM zaferan_sofreh.gold_v2.daily_restaurant_reviews s
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r ON s.restaurant_id = r.restaurant_id
GROUP BY r.name, s.restaurant_id

UNION ALL

SELECT
  COALESCE(r.name, s.restaurant_id) AS restaurant_name,
  '4 Stars' AS rating_label, 4 AS star_rank, SUM(s.rating_4_stars) AS rating_count
FROM zaferan_sofreh.gold_v2.daily_restaurant_reviews s
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r ON s.restaurant_id = r.restaurant_id
GROUP BY r.name, s.restaurant_id

UNION ALL

SELECT
  COALESCE(r.name, s.restaurant_id) AS restaurant_name,
  '3 Stars' AS rating_label, 3 AS star_rank, SUM(s.rating_3_stars) AS rating_count
FROM zaferan_sofreh.gold_v2.daily_restaurant_reviews s
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r ON s.restaurant_id = r.restaurant_id
GROUP BY r.name, s.restaurant_id

UNION ALL

SELECT
  COALESCE(r.name, s.restaurant_id) AS restaurant_name,
  '2 Stars' AS rating_label, 2 AS star_rank, SUM(s.rating_2_stars) AS rating_count
FROM zaferan_sofreh.gold_v2.daily_restaurant_reviews s
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r ON s.restaurant_id = r.restaurant_id
GROUP BY r.name, s.restaurant_id

UNION ALL

SELECT
  COALESCE(r.name, s.restaurant_id) AS restaurant_name,
  '1 Star' AS rating_label, 1 AS star_rank, SUM(s.rating_1_stars) AS rating_count
FROM zaferan_sofreh.gold_v2.daily_restaurant_reviews s
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r ON s.restaurant_id = r.restaurant_id
GROUP BY r.name, s.restaurant_id;