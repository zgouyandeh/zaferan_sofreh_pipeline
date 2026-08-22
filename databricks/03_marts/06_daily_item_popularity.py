"""
databricks/03_marts/06_daily_item_popularity.py
------------------------------------------------------      
This code reads the silver fact_order_items, fact_orders, and dim_menu_item tables, applies aggregations and quality checks,
and writes the result to a daily_item_popularity materialized view.
It creates a daily aggregated view of dish sales, including order counts, total quantity sold, total revenue, and average unit price,
which can be used for dynamic dashboard filtering and analysis of item popularity trends over time.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="daily_item_popularity",
    partition_cols=["order_date"],
    table_properties={"quality": "gold"},
    comment="Daily aggregated dish sales for dynamic dashboard filtering",
)
def daily_item_popularity():
    # ============================================================
    # 1. Load Silver tables
    # ============================================================

    df_items = dp.read("zaferan_sofreh.silver.fact_order_items").drop("order_date")
    df_orders = dp.read("zaferan_sofreh.silver.fact_orders")
    df_menu = dp.read("zaferan_sofreh.silver.dim_menu_item")
    # ============================================================
    # 2. Select valid completed/delivered orders
    # ============================================================

    valid_orders = (
        df_orders
        .filter(F.col("order_status").isin("completed", "delivered"))
        .select("order_id", "order_date", "restaurant_id")
    )
    
    
    # ============================================================
    # 3. Join on BOTH order_id AND restaurant_id to remove column duplication
    # ============================================================

    valid_items = df_items.join(
        valid_orders, 
        on=["order_id", "restaurant_id"], 
        how="inner"
    )
    # ============================================================
    # 4. Daily aggregation
    # ============================================================

    daily_stats = (
        valid_items
        .groupBy("order_date", "restaurant_id", "item_id")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("total_quantity_sold"),
            F.round(F.sum("subtotal"), 2).alias("total_revenue"),
            F.round(F.avg("unit_price"), 2).alias("avg_unit_price")
        )
    )
    # ============================================================
    # 5. Attach menu dimension details
    # ============================================================

    return (
        daily_stats
        .join(
            df_menu.select("restaurant_id", "item_id", F.col("name").alias("item_name"), "category", "is_vegetarian"),
            on=["restaurant_id", "item_id"],
            how="inner"
        )
    )