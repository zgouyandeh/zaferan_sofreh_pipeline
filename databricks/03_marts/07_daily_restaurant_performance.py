"""
databricks/03_marts/07_daily_restaurant_performance.py
------------------------------------------------------
This code reads the silver fact_orders, fact_review, and dim_restaurant tables,
applies aggregations and quality checks, and writes the result to a
daily_restaurant_performance table.
It creates a daily aggregated view of restaurant performance, including order
counts, revenue, unique customers, average order value, and review statistics,
which can be used for dynamic dashboard filtering and analysis of restaurant
performance trends over time.
"""

from pyspark.sql import functions as F
from pyspark import pipelines as dp


@dp.table(
    name="daily_restaurant_performance",
    table_properties={"quality": "gold"}
)
def daily_restaurant_performance():

    df_orders = spark.table("zaferan_sofreh.silver.fact_orders")
    df_reviews = spark.table("zaferan_sofreh.silver.fact_review")
    df_restaurants = spark.table("zaferan_sofreh.silver.dim_restaurant")

    valid_orders = (
        df_orders.filter(F.col("order_status").isin("completed", "delivered"))
    )

    # =====================================================
    # Daily restaurant sales performance
    # =====================================================

    daily_sales = (
        valid_orders
        .groupBy("restaurant_id", "order_date")
        .agg(
            F.countDistinct("order_id").alias("daily_orders"),
            F.round(F.sum("total_amount"), 2).alias("daily_revenue"),
            F.countDistinct("customer_id").alias("daily_unique_customers"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.countDistinct(
                F.when(F.col("order_type") == "delivery", F.col("order_id"))
            ).alias("delivery_orders"),
            F.sum(
                F.when(F.col("order_type") == "delivery", F.col("total_amount"))
            ).alias("delivery_revenue"),
        )
        .withColumnRenamed("order_date", "activity_date")
    )

    # =====================================================
    # Daily review performance
    # =====================================================

    daily_reviews = (
        df_reviews
        .withColumn("activity_date", F.to_date("review_timestamp"))
        .groupBy("restaurant_id", "activity_date")
        .agg(
            F.countDistinct("review_id").alias("daily_reviews"),
            F.round(F.avg("rating"), 2).alias("daily_avg_rating"),
            F.round(
                F.avg(F.when(F.col("sentiment") == "positive", 1).otherwise(0)), 4
            ).alias("daily_positive_ratio"),
            F.round(
                F.avg(F.when(F.col("sentiment") == "negative", 1).otherwise(0)), 4
            ).alias("daily_negative_ratio"),
        )
    )

    # =====================================================
    # Date scaffold — one row per (restaurant, day) for every
    # day from that restaurant's first real order through today,
    # so days with zero orders show up as 0, not as a gap.
    # =====================================================

    restaurant_date_bounds = (
        valid_orders
        .groupBy("restaurant_id")
        .agg(F.min("order_date").alias("first_order_date"))
    )

    date_scaffold = (
        restaurant_date_bounds
        .withColumn(
            "activity_date",
            F.explode(F.sequence(F.col("first_order_date"), F.current_date())),
        )
        .select("restaurant_id", "activity_date")
    )

    # =====================================================
    # Restaurant static context — for cross-restaurant pooling
    # in the future panel forecasting model. opening_date is
    # fixed per restaurant; restaurant_age_days is therefore
    # a deterministic, non-leaky function of activity_date and
    # is safe to use as a forecasting feature at any horizon.
    # =====================================================

    restaurant_context = (
        df_restaurants
        .select(
            "restaurant_id",
            F.col("city").alias("restaurant_city"),
            "opening_date",
        )
    )

    # =====================================================
    # Calendar features — same convention as fact_orders
    # (Friday = weekend), computed once here.
    # =====================================================

    calendar_features = date_scaffold.withColumn(
        "day_of_week", F.date_format(F.col("activity_date"), "EEEE")
    ).withColumn(
        "is_weekend", F.when(F.col("day_of_week") == "Friday", True).otherwise(False)
    )

    # =====================================================
    # Final daily performance table
    # =====================================================

    return (
        calendar_features
        .join(daily_sales, on=["restaurant_id", "activity_date"], how="left")
        .join(daily_reviews, on=["restaurant_id", "activity_date"], how="left")
        .join(restaurant_context, on="restaurant_id", how="left")
        .withColumn(
            "restaurant_age_days",
            F.datediff(F.col("activity_date"), F.col("opening_date")),
        )
        .drop("opening_date")
        .fillna(
            {
                "daily_orders": 0,
                "daily_revenue": 0,
                "daily_unique_customers": 0,
                "delivery_orders": 0,
                "delivery_revenue": 0,
                "daily_reviews": 0,
            }
        )
    )