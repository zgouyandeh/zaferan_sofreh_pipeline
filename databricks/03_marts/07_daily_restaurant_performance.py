from pyspark.sql import functions as F
from pyspark import pipelines as dp


@dp.table(
    name="daily_restaurant_performance",
    table_properties={"quality": "gold"}
)
def daily_restaurant_performance():

    df_orders = spark.table("zaferan_sofreh.silver.fact_orders")
    df_reviews = spark.table("zaferan_sofreh.silver.fact_review")

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
    #
    # NOTE: joined on activity_date, but this reflects when a
    # review was SUBMITTED, not when the order it discusses was
    # placed (reviews land 1-7 days after their order per the
    # generator). This is "review volume that day," not "reviews
    # about orders placed that day" — don't read it as the latter.
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
    # Final daily performance table
    # =====================================================

    return (
        date_scaffold
        .join(daily_sales, on=["restaurant_id", "activity_date"], how="left")
        .join(daily_reviews, on=["restaurant_id", "activity_date"], how="left")
        .fillna(
            {
                "daily_orders": 0,
                "daily_revenue": 0,
                "daily_unique_customers": 0,
                "delivery_orders": 0,
                "delivery_revenue": 0,
                "daily_reviews": 0,
            }
            # avg_order_value, daily_avg_rating, daily_positive_ratio,
            # daily_negative_ratio deliberately NOT filled — a day
            # with zero orders/reviews has no meaningful average,
            # that's genuinely null, not 0.
        )
    )