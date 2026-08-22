SELECT
  COALESCE(category, 'Uncategorized') AS category,
  SUM(total_revenue) AS category_revenue
FROM zaferan_sofreh.gold_v2.daily_item_popularity
WHERE ( :date_range.min IS NULL OR order_date >= DATE(:date_range.min) )
  AND ( :date_range.max IS NULL OR order_date <= DATE(:date_range.max) )
GROUP BY 1
HAVING category_revenue > 0
ORDER BY category_revenue DESC;