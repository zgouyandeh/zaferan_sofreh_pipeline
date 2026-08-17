from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark import pipelines as dp


@dp.table(
    name="item_popularity",
    table_properties={"quality": "gold"}
)
def item_popularity():

    df_items = spark.table("zaferan_sofreh.silver.fact_order_items").drop("order_date")
    df_orders = spark.table("zaferan_sofreh.silver.fact_orders")
    df_menu = spark.table("zaferan_sofreh.silver.dim_menu_item")

    # =====================================================
    # Valid sales only
    # =====================================================

    valid_order_ids = (
        df_orders
        .filter(F.col("order_status").isin("completed", "delivered"))
        .select("order_id", "order_date")
    )

    valid_items = df_items.join(valid_order_ids, on="order_id", how="inner")

    # =====================================================
    # Item performance
    # =====================================================

    item_stats = (
        valid_items
        .groupBy("restaurant_id", "item_id")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("total_quantity_sold"),
            F.round(F.sum("subtotal"), 2).alias("total_revenue"),
            F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
            F.max("order_date").alias("last_sold_date"),
        )
    )

    # =====================================================
    # Restaurant totals — for both quantity share and revenue share
    # =====================================================

    restaurant_totals = (
        item_stats
        .groupBy("restaurant_id")
        .agg(
            F.sum("total_quantity_sold").alias("restaurant_total_items_sold"),
            F.sum("total_revenue").alias("restaurant_total_revenue"),
        )
    )

    result = (
        item_stats
        .join(restaurant_totals, on="restaurant_id", how="left")
        .withColumn(
            "item_sales_share",
            F.round(F.col("total_quantity_sold") / F.col("restaurant_total_items_sold"), 4),
        )
        .withColumn(
            "item_revenue_share",
            F.round(F.col("total_revenue") / F.col("restaurant_total_revenue"), 4),
        )
    )

    # =====================================================
    # Ranking
    # FIX: row_number() instead of dense_rank(), with a
    # tiebreaker, so "top N" always means exactly N items and
    # ranks never skip in a confusing way.
    # =====================================================

    popularity_window = Window.partitionBy("restaurant_id")

    result = (
        result
        .withColumn(
            "popularity_rank",
            F.row_number().over(
                popularity_window.orderBy(F.desc("total_quantity_sold"), F.desc("total_revenue"))
            ),
        )
        .withColumn(
            "revenue_rank",
            F.row_number().over(
                popularity_window.orderBy(F.desc("total_revenue"), F.desc("total_quantity_sold"))
            ),
        )
    )

    # =====================================================
    # Add menu information — LEFT join from the full menu,
    # not from result, so items that never sold still appear.
    # =====================================================

    menu_with_stats = (
        df_menu
        .select(
            "restaurant_id", "item_id",
            F.col("name").alias("item_name"),
            "category", "is_vegetarian",
        )
        .join(result, on=["restaurant_id", "item_id"], how="left")
    )

    # =====================================================
    # Final output
    # =====================================================

    return (
        menu_with_stats
        .fillna(
            {
                "order_count": 0,
                "total_quantity_sold": 0,
                "total_revenue": 0,
                "item_sales_share": 0,
                "item_revenue_share": 0,
            }
        )
        # avg_unit_price, last_sold_date, popularity_rank, revenue_rank
        # deliberately NOT filled — a never-sold item has no price
        # history and no rank; null correctly signals "no data",
        # not "zero".
        .withColumn(
            "days_since_last_sale",
            F.datediff(F.current_date(), F.col("last_sold_date")),
        )
        .withColumn(
            "is_never_sold",
            F.col("order_count") == 0,
        )
        .select(
            "restaurant_id",
            "item_id",
            "item_name",
            "category",
            "is_vegetarian",
            "order_count",
            "total_quantity_sold",
            "item_sales_share",
            "total_revenue",
            "item_revenue_share",
            "avg_unit_price",
            "popularity_rank",
            "revenue_rank",
            "last_sold_date",
            "days_since_last_sale",
            "is_never_sold",
        )
    )