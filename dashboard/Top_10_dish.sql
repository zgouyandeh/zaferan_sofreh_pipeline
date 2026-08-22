SELECT
  item_name,
  SUM(total_quantity_sold) AS total_units_sold
FROM zaferan_sofreh.gold_v2.daily_item_popularity
WHERE ( :date_range.min IS NULL OR order_date >= DATE(:date_range.min) )
  AND ( :date_range.max IS NULL OR order_date <= DATE(:date_range.max) )
GROUP BY item_name
ORDER BY total_units_sold DESC
LIMIT 10;