"""
databrikcs/03_marts/03_customer_restaurant_preference.py
------------------------------------------------------
This code reads the silver fact_orders and fact_review tables, applies aggregations and quality checks, 
and writes the result to a customer_restaurant_preference table.
It calculates a preference score for each customer-restaurant pair based on order history, recency, and review sentiment, and ranks restaurants for each customer.

"""

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark import pipelines as dp


# ============================================================
# Helper
# ============================================================

def severity_to_score(column_name):
    """
    Convert issue severity into a numeric score.

    none     -> 0
    minor    -> 1
    moderate -> 2
    severe   -> 3
    """
    return (
        F.when(F.col(column_name) == "minor", 1.0)
         .when(F.col(column_name) == "moderate", 2.0)
         .when(F.col(column_name) == "severe", 3.0)
         .otherwise(0.0)
    )


# ============================================================
# Gold Table
# ============================================================

@dp.table(
    name="customer_restaurant_preference",
    table_properties={"quality": "gold"}
)
def customer_restaurant_preference():

    # ========================================================
    # 1. Read Silver tables
    # ========================================================

    df_orders = spark.table("zaferan_sofreh.silver.fact_orders")
    df_reviews = spark.table("zaferan_sofreh.silver.fact_review")

    # ========================================================
    # 2. Valid orders — only successful orders contribute to
    #    restaurant preference.
    # ========================================================

    valid_orders = (
        df_orders
        .filter(F.col("order_status").isin("completed", "delivered"))
    )

    # ========================================================
    # 3. Customer-level order statistics (for order_share)
    # ========================================================

    customer_total_orders = (
        valid_orders
        .groupBy("customer_id")
        .agg(F.countDistinct("order_id").alias("customer_total_orders"))
    )

    # ========================================================
    # 4. Customer x Restaurant order profile
    # ========================================================

    customer_restaurant_orders = (
        valid_orders
        .groupBy("customer_id", "restaurant_id")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("total_amount").alias("total_spend"),
            F.min("order_timestamp").alias("first_order_timestamp"),
            F.max("order_timestamp").alias("last_order_timestamp"),
        )
        .join(customer_total_orders, on="customer_id", how="left")
        .withColumn(
            "order_share",
            F.round(F.col("order_count") / F.col("customer_total_orders"), 4),
        )
        .withColumn(
            "days_since_last_order",
            F.datediff(F.current_date(), F.to_date("last_order_timestamp")),
        )
    )

    # ========================================================
    # 5. Customer x Restaurant review profile
    # ========================================================

    customer_restaurant_reviews = (
        df_reviews
        .groupBy("customer_id", "restaurant_id")
        .agg(
            F.countDistinct("review_id").alias("review_count"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.round(
                F.avg(F.when(F.col("sentiment") == "positive", 1.0).otherwise(0.0)), 4
            ).alias("positive_ratio"),
            F.round(
                F.avg(F.when(F.col("sentiment") == "negative", 1.0).otherwise(0.0)), 4
            ).alias("negative_ratio"),
            F.round(F.avg(severity_to_score("food_quality_severity")), 4)
             .alias("avg_food_quality_severity"),
            F.round(F.avg(severity_to_score("pricing_severity")), 4)
             .alias("avg_pricing_severity"),
            F.round(F.avg(severity_to_score("portion_size_severity")), 4)
             .alias("avg_portion_severity"),
            # Delivery is intentionally kept for analysis but will NOT
            # be used in preference_score.
            F.round(F.avg(severity_to_score("delivery_severity")), 4)
             .alias("avg_delivery_severity"),
        )
    )

    # ========================================================
    # 6. Combine order + review profiles
    # ========================================================

    profile = (
        customer_restaurant_orders
        .join(customer_restaurant_reviews, on=["customer_id", "restaurant_id"], how="left")
        .fillna(
            {
                "review_count": 0,
                "positive_ratio": 0.0,
                "negative_ratio": 0.0,
            }
        )
    )

    # ========================================================
    # 7. Eligibility — a restaurant needs at least 2 orders from
    #    the customer to be considered a candidate.
    #    IMPORTANT: we do NOT delete rows that fail this rule.
    # ========================================================

    profile = profile.withColumn("is_eligible", F.col("order_count") >= 2)

    # ========================================================
    # 8. Customer-level eligibility information
    # ========================================================

    customer_window = Window.partitionBy("customer_id")

    profile = (
        profile
        .withColumn(
            "eligible_restaurant_count",
            F.sum(F.when(F.col("is_eligible"), 1).otherwise(0)).over(customer_window),
        )
        .withColumn("has_sufficient_data", F.col("eligible_restaurant_count") > 0)
    )

    # ========================================================
    # 9. Recency score — exponential decay, avoids one very old
    #    restaurant distorting every other row's score.
    # ========================================================

    decay_days = 90.0

    profile = profile.withColumn(
        "recency_score",
        F.exp(-F.col("days_since_last_order") / F.lit(decay_days)),
    )

    # ========================================================
    # 10. Rating score — raw 1-5 rating mapped to ~0-1
    # ========================================================

    profile = profile.withColumn(
        "rating_score_raw",
        (F.col("avg_rating") - 1.0) / 4.0,
    )

    # ========================================================
    # 11. Review confidence — smoothing so 1 review isn't as
    #     trustworthy as 20 reviews.
    # ========================================================

    smoothing_reviews = 3.0

    profile = profile.withColumn(
        "review_confidence",
        F.col("review_count") / (F.col("review_count") + F.lit(smoothing_reviews)),
    )

    # ========================================================
    # 12. Smoothed rating score — no reviews -> neutral 0.5,
    #     more reviews -> actual rating matters more.
    # ========================================================

    profile = profile.withColumn(
        "rating_score",
        F.coalesce(
            (F.col("review_confidence") * F.col("rating_score_raw"))
            + ((1 - F.col("review_confidence")) * F.lit(0.5)),
            F.lit(0.5),
        ),
    )

    # ========================================================
    # 13. Sentiment score — -1..+1 mapped to 0..1
    # ========================================================

    profile = (
        profile
        .withColumn(
            "sentiment_score_raw",
            F.col("positive_ratio") - F.col("negative_ratio"),
        )
        .withColumn(
            "sentiment_score",
            (F.col("sentiment_score_raw") + 1.0) / 2.0,
        )
    )

    # ========================================================
    # 14. Food quality / pricing / portion scores
    # ========================================================

    profile = (
        profile
        .withColumn(
            "food_quality_score_raw",
            1.0 - (F.col("avg_food_quality_severity") / 3.0),
        )
        .withColumn(
            "pricing_score_raw",
            1.0 - (F.col("avg_pricing_severity") / 3.0),
        )
        .withColumn(
            "portion_score_raw",
            1.0 - (F.col("avg_portion_severity") / 3.0),
        )
        .withColumn(
            "food_quality_score",
            F.coalesce(
                (F.col("review_confidence") * F.col("food_quality_score_raw"))
                + ((1 - F.col("review_confidence")) * F.lit(0.5)),
                F.lit(0.5),
            ),
        )
        .withColumn(
            "pricing_score",
            F.coalesce(
                (F.col("review_confidence") * F.col("pricing_score_raw"))
                + ((1 - F.col("review_confidence")) * F.lit(0.5)),
                F.lit(0.5),
            ),
        )
        .withColumn(
            "portion_score",
            F.coalesce(
                (F.col("review_confidence") * F.col("portion_score_raw"))
                + ((1 - F.col("review_confidence")) * F.lit(0.5)),
                F.lit(0.5),
            ),
        )
    )

    # ========================================================
    # 15. Overall restaurant experience score
    #
    # Rating -> 50%, Sentiment -> 20%, Food quality -> 15%,
    # Pricing -> 10%, Portion -> 5%. Delivery NOT included.
    # ========================================================

    profile = profile.withColumn(
        "experience_score",
        F.round(
            0.50 * F.col("rating_score")
            + 0.20 * F.col("sentiment_score")
            + 0.15 * F.col("food_quality_score")
            + 0.10 * F.col("pricing_score")
            + 0.05 * F.col("portion_score"),
            4,
        ),
    )

    # ========================================================
    # 16. Final preference score
    #
    # Behavior -> 40%, Recency -> 20%, Experience -> 40%
    # ========================================================

    profile = profile.withColumn(
        "preference_score",
        F.round(
            0.40 * F.col("order_share")
            + 0.20 * F.col("recency_score")
            + 0.40 * F.col("experience_score"),
            4,
        ),
    )

    # ========================================================
    # 17. Rank restaurants for each customer
    # ========================================================

    preference_window = (
        Window
        .partitionBy("customer_id")
        .orderBy(
            F.desc(F.col("is_eligible").cast("int")),
            F.desc("preference_score"),
            F.desc("order_count"),      # tiebreaker 1
            F.desc("total_spend"),      # tiebreaker 2
        )
    )

    profile = profile.withColumn(
        "preference_rank",
        F.when(F.col("is_eligible"), F.row_number().over(preference_window)),
    )

    # ========================================================
    # 18. Final status
    #
    # insufficient_data: customer has no restaurant with >= 2 orders
    # preferred:         eligible restaurant with rank = 1
    # candidate:         eligible restaurant but not rank 1
    # not_eligible:      restaurant has only 1 order, but customer
    #                    has enough data elsewhere
    # ========================================================

    profile = profile.withColumn(
        "preference_status",
        F.when(~F.col("has_sufficient_data"), F.lit("insufficient_data"))
         .when(F.col("is_eligible") & (F.col("preference_rank") == 1), F.lit("preferred"))
         .when(F.col("is_eligible"), F.lit("candidate"))
         .otherwise(F.lit("not_eligible")),
    )

    # ========================================================
    # 19. Final output
    # ========================================================

    return (
        profile
        .select(
            "customer_id",
            "restaurant_id",
            "order_count",
            "customer_total_orders",
            "order_share",
            "total_spend",
            "first_order_timestamp",
            "last_order_timestamp",
            "days_since_last_order",
            "review_count",
            "avg_rating",
            "positive_ratio",
            "negative_ratio",
            "avg_food_quality_severity",
            "avg_pricing_severity",
            "avg_portion_severity",
            "avg_delivery_severity",
            "recency_score",
            "rating_score",
            "sentiment_score",
            "food_quality_score",
            "pricing_score",
            "portion_score",
            "experience_score",
            "preference_score",
            "is_eligible",
            "eligible_restaurant_count",
            "preference_rank",
            "preference_status",
        )
    )