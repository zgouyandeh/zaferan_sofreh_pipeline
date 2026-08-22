"""
databricks/03_marts/04_customer_360.py
------------------------------------------------------
This code reads the silver dim_customer, fact_orders, fact_review, and gold customer_restaurant_preference tables, 
applies aggregations and quality checks, and writes the result to a customer_360 table.
It creates a comprehensive 360-degree view of each customer, including order history, review behavior,
and preferred restaurant information, which can be used for personalized marketing, customer segmentation, and loyalty programs.

"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(name="customer_360", table_properties={"quality": "gold"})
def customer_360():
# ============================================================
# Load Silver / Gold tables
# ============================================================
    df_customers = spark.table("zaferan_sofreh.silver.dim_customer")
    df_orders = spark.table("zaferan_sofreh.silver.fact_orders")
    df_reviews = spark.table("zaferan_sofreh.silver.fact_review")
    df_pref = spark.table(
        "zaferan_sofreh.gold_v2.customer_restaurant_preference"
    )
    df_restaurants = spark.table("zaferan_sofreh.silver.dim_restaurant")
# ============================================================
    # 1. Customer Order Profile
# ============================================================
    valid_orders = df_orders.filter(
        F.col("order_status").isin("completed", "delivered")
    )

    customer_orders = (
        valid_orders.groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("total_amount"), 2).alias("lifetime_spend"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.max("order_date").alias("last_order_date"),
            F.round(F.avg("order_hour"), 1).alias("avg_order_hour"),
            F.round(
                F.avg(F.when(F.col("is_weekend"), 1).otherwise(0)), 4
            ).alias("weekend_order_ratio"),
        )
        .withColumn(
            "days_since_last_order",
            F.datediff(F.current_date(), F.col("last_order_date")),
        )
        .withColumn(
            "loyalty_segment",
            F.when(F.col("lifetime_spend") >= 5000000, "Platinum")
            .when(F.col("lifetime_spend") >= 2000000, "Gold")
            .when(F.col("lifetime_spend") >= 1000000, "Silver")
            .otherwise("Bronze"),
        )
    )
# ============================================================
    # 2. Customer Review Profile
# ============================================================
    customer_reviews = df_reviews.groupBy("customer_id").agg(
        F.countDistinct("review_id").alias("total_reviews_given"),
        F.round(F.avg("rating"), 2).alias("avg_rating_given"),
        F.round(
            F.avg(F.when(F.col("sentiment") == "positive", 1).otherwise(0)), 4
        ).alias("positive_review_ratio"),
        F.round(
            F.avg(F.when(F.col("sentiment") == "negative", 1).otherwise(0)), 4
        ).alias("negative_review_ratio"),
        F.round(
            F.avg(F.when(F.col("issue_food_quality"), 1).otherwise(0)), 4
        ).alias("food_quality_issue_ratio"),
        F.round(
            F.avg(F.when(F.col("issue_pricing"), 1).otherwise(0)), 4
        ).alias("pricing_issue_ratio"),
        F.round(
            F.avg(F.when(F.col("issue_portion_size"), 1).otherwise(0)), 4
        ).alias("portion_issue_ratio"),
    )
# ============================================================
    # 3. Preferred Restaurant
# ============================================================
    preferred_restaurant = (
        df_pref.filter(F.col("preference_rank") == 1)
        .join(
            df_restaurants.select(
                "restaurant_id",
                F.col("name").alias("preferred_restaurant_name"),
            ),
            on="restaurant_id",
            how="left",
        )
        .select(
            "customer_id",
            F.col("restaurant_id").alias("preferred_restaurant_id"),
            "preferred_restaurant_name",
            "preference_score",
            "order_share",
        )
    )
# ============================================================
    # 4. Customer 360 Final Profile
# ============================================================
    return (
        df_customers.select("customer_id")
        .join(customer_orders, on="customer_id", how="left")
        .join(customer_reviews, on="customer_id", how="left")
        .join(preferred_restaurant, on="customer_id", how="left")
        .fillna(
            {
                "total_orders": 0,
                "lifetime_spend": 0,
                "total_reviews_given": 0,
                "positive_review_ratio": 0,
                "negative_review_ratio": 0,
                "food_quality_issue_ratio": 0,
                "pricing_issue_ratio": 0,
                "portion_issue_ratio": 0,
            }
        )
        .withColumn(
            "loyalty_segment",
            F.coalesce(F.col("loyalty_segment"), F.lit("No Orders Yet")),
        )
        .withColumn(
            "customer_preference_status",
            F.when(
                F.col("preferred_restaurant_id").isNotNull(), "has_preference"
            )
            .when(F.col("total_orders") == 0, "no_orders_yet")
            .otherwise("insufficient_data"),
        )
        .withColumn(
            "customer_activity_status",
            F.when(F.col("days_since_last_order") <= 30, "active")
            .when(F.col("days_since_last_order") <= 90, "at_risk")
            .when(F.col("days_since_last_order") > 90, "churned")
            .otherwise("no_orders_yet"),
        )
    )