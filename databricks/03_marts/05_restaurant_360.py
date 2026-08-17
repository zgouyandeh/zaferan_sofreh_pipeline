from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark import pipelines as dp


def severity_to_score(col_name):
    """None for 'none' is intentional — this should only average
    severity AMONG rows that actually have the issue. Complaint
    frequency is tracked separately via *_complaint_count below."""
    return (
        F.when(F.col(col_name) == "minor", 1.0)
        .when(F.col(col_name) == "moderate", 2.0)
        .when(F.col(col_name) == "severe", 3.0)
        .otherwise(None)
    )


@dp.table(
    name="restaurant_360",
    table_properties={"quality": "gold"}
)
def restaurant_360():

    df_orders = spark.table("zaferan_sofreh.silver.fact_orders")
    df_reviews = spark.table("zaferan_sofreh.silver.fact_review")
    df_restaurants = spark.table("zaferan_sofreh.silver.dim_restaurant")

    valid_orders = (
        df_orders.filter(F.col("order_status").isin("completed", "delivered"))
    )

    # ======================================================
    # 1. Customer loyalty statistics
    # FIX: avg_orders_per_customer now lives directly on
    # repeat_stats, so it actually reaches the final table.
    # ======================================================

    customer_order_counts = (
        valid_orders
        .groupBy("restaurant_id", "customer_id")
        .agg(F.countDistinct("order_id").alias("orders_from_this_customer"))
    )

    repeat_stats = (
        customer_order_counts
        .groupBy("restaurant_id")
        .agg(
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct(
                F.when(F.col("orders_from_this_customer") >= 2, F.col("customer_id"))
            ).alias("repeat_customers"),
            F.sum("orders_from_this_customer").alias("total_orders_check"),  # sanity cross-check only
        )
        .withColumn(
            "repeat_customer_rate",
            F.round(F.col("repeat_customers") / F.col("unique_customers"), 4),
        )
        .drop("total_orders_check")
    )

    # ======================================================
    # 2. General order statistics
    # ======================================================

    order_stats = (
        valid_orders
        .groupBy("restaurant_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.max("order_date").alias("last_order_date"),
        )
        .withColumn(
            "days_since_last_order",
            F.datediff(F.current_date(), F.col("last_order_date")),
        )
    )

    # ======================================================
    # 3. Delivery operational statistics
    # ======================================================

    delivery_order_stats = (
        valid_orders
        .filter(F.col("order_type") == "delivery")
        .groupBy("restaurant_id")
        .agg(
            F.countDistinct("order_id").alias("delivery_order_count"),
            F.sum("total_amount").alias("delivery_revenue"),
        )
    )

    # ======================================================
    # 4. Review analytics
    #
    # Each issue category now gets BOTH signals:
    #   - *_complaint_count: how often it happens (frequency)
    #   - avg_*_severity: how bad it is WHEN it happens (intensity)
    # ======================================================

    review_stats = (
        df_reviews
        .groupBy("restaurant_id")
        .agg(
            F.countDistinct("review_id").alias("total_reviews"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.round(F.avg(F.when(F.col("sentiment") == "positive", 1).otherwise(0)), 4)
             .alias("positive_ratio"),
            F.round(F.avg(F.when(F.col("sentiment") == "negative", 1).otherwise(0)), 4)
             .alias("negative_ratio"),

            F.countDistinct(F.when(F.col("issue_delivery") == True, F.col("review_id")))
             .alias("delivery_complaint_count"),
            F.round(F.avg(severity_to_score("delivery_severity")), 2)
             .alias("avg_delivery_severity"),

            F.countDistinct(F.when(F.col("issue_food_quality") == True, F.col("review_id")))
             .alias("food_quality_complaint_count"),
            F.round(F.avg(severity_to_score("food_quality_severity")), 2)
             .alias("avg_food_quality_severity"),

            F.countDistinct(F.when(F.col("issue_pricing") == True, F.col("review_id")))
             .alias("pricing_complaint_count"),
            F.round(F.avg(severity_to_score("pricing_severity")), 2)
             .alias("avg_pricing_severity"),

            F.countDistinct(F.when(F.col("issue_portion_size") == True, F.col("review_id")))
             .alias("portion_complaint_count"),
            F.round(F.avg(severity_to_score("portion_size_severity")), 2)
             .alias("avg_portion_severity"),
        )
        .withColumn(
            "review_confidence",
            F.round(F.col("total_reviews") / (F.col("total_reviews") + F.lit(10)), 4),
        )
    )

    # ======================================================
    # 5. Build restaurant profile
    # ======================================================

    restaurant_profile = (
        df_restaurants
        .select("restaurant_id", F.col("name").alias("restaurant_name"), "city")
        .join(order_stats, on="restaurant_id", how="left")
        .join(repeat_stats, on="restaurant_id", how="left")
        .join(delivery_order_stats, on="restaurant_id", how="left")
        .join(review_stats, on="restaurant_id", how="left")
    )

    # ======================================================
    # 6. Fill true zeros BEFORE deriving ratios from them
    #
    # FIX: this now happens before delivery_order_ratio is
    # computed, so a restaurant with orders but zero delivery
    # orders correctly gets 0.0, not null.
    # ======================================================

    restaurant_profile = restaurant_profile.fillna(
        {
            "total_orders": 0,
            "total_revenue": 0,
            "unique_customers": 0,
            "repeat_customers": 0,
            "repeat_customer_rate": 0,
            "delivery_order_count": 0,
            "delivery_revenue": 0,
            "total_reviews": 0,
            "review_confidence": 0,
            "delivery_complaint_count": 0,
            "food_quality_complaint_count": 0,
            "pricing_complaint_count": 0,
            "portion_complaint_count": 0,
            "positive_ratio": 0,
            "negative_ratio": 0,
        }
        # avg_order_value, last_order_date, days_since_last_order,
        # avg_rating, avg_*_severity deliberately NOT filled — these
        # are genuinely unknown with no data, not zero.
    )

    # ======================================================
    # 7. Derived metrics
    # ======================================================

    restaurant_profile = (
        restaurant_profile
        .withColumn(
            "avg_orders_per_customer",
            F.when(
                F.col("unique_customers") > 0,
                F.round(F.col("total_orders") / F.col("unique_customers"), 2),
            ),
        )
        .withColumn(
            "delivery_order_ratio",
            F.when(
                F.col("total_orders") > 0,
                F.round(F.col("delivery_order_count") / F.col("total_orders"), 4),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("rating_score_raw", (F.col("avg_rating") - 1) / 4)
        .withColumn("quality_score_raw", 1 - (F.col("avg_food_quality_severity") / 3))
        # Confidence-weighted toward neutral 0.5 — same pattern as
        # customer_restaurant_preference, so a restaurant with 1
        # review can't swing to a perfect or terrible score outright.
        .withColumn(
            "rating_score",
            F.coalesce(
                F.col("review_confidence") * F.col("rating_score_raw")
                + (1 - F.col("review_confidence")) * F.lit(0.5),
                F.lit(0.5),
            ),
        )
        .withColumn(
            "quality_score",
            F.coalesce(
                F.col("review_confidence") * F.col("quality_score_raw")
                + (1 - F.col("review_confidence")) * F.lit(0.5),
                F.lit(0.5),
            ),
        )
        .withColumn(
            "restaurant_health_score",
            F.round(
                0.35 * F.col("rating_score")
                + 0.25 * F.col("repeat_customer_rate")
                + 0.20 * F.col("positive_ratio")
                + 0.20 * F.col("quality_score"),
                4,
            ),
        )
        .drop("rating_score_raw", "quality_score_raw")
    )

    return restaurant_profile