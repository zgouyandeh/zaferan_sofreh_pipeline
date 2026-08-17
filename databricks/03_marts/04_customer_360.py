from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark import pipelines as dp


@dp.table(
    name="customer_360",
    table_properties={"quality": "gold"}
)
def customer_360():

    # =====================================================
    # Load Silver / Gold tables
    # =====================================================

    df_customers = spark.table( "zaferan_sofreh.silver.dim_customer" ) 

    df_orders = spark.table("zaferan_sofreh.silver.fact_orders" )

    df_reviews = spark.table("zaferan_sofreh.silver.fact_review")

    df_pref = spark.table("zaferan_sofreh.gold_v2.customer_restaurant_preference"    )

    df_restaurants = spark.table("zaferan_sofreh.silver.dim_restaurant")


    # =====================================================
    # 1. Customer Order Profile
    # =====================================================

    valid_orders = (df_orders.filter(F.col("order_status").isin("completed","delivered" )))


    customer_orders = (

        valid_orders

        .groupBy(
            "customer_id"
        )

        .agg(

            # Frequency
            F.countDistinct("order_id")
            .alias("total_orders"),


            # Monetary
            F.round(
                F.sum("total_amount"),
                2
            )
            .alias("lifetime_spend"),


            F.round(
                F.avg("total_amount"),
                2
            )
            .alias("avg_order_value"),


            # Recency
            F.max("order_date")
            .alias("last_order_date"),


            # Preferred ordering behavior
            F.round(
                F.avg("order_hour"),
                1
            )
            .alias("avg_order_hour"),


            # Ratio, not just a flag — how much of this
            # customer's ordering happens on weekends.
            F.round(
                F.avg(
                    F.when(
                        F.col("is_weekend"),
                        1
                    )
                    .otherwise(0)
                ),
                4
            )
            .alias("weekend_order_ratio")

        )


        .withColumn(
            "days_since_last_order",
            F.datediff(
                F.current_date(),
                F.col("last_order_date")
            )
        )


        # -----------------------------
        # Loyalty segmentation
        # -----------------------------

        .withColumn(
            "loyalty_segment",

            F.when(
                F.col("lifetime_spend") >= 5000000,
                "Platinum"
            )

            .when(
                F.col("lifetime_spend") >= 2000000,
                "Gold"
            )

            .when(
                F.col("lifetime_spend") >= 1000000,
                "Silver"
            )

            .otherwise(
                "Bronze"
            )
        )
    )



    # =====================================================
    # 2. Customer Review Profile
    # =====================================================


    customer_reviews = (

        df_reviews

        .groupBy(
            "customer_id"
        )

        .agg(

            F.countDistinct("review_id")
            .alias(
                "total_reviews_given"
            ),


            F.round(
                F.avg("rating"),
                2
            )
            .alias(
                "avg_rating_given"
            ),


            # Sentiment behaviour

            F.round(
                F.avg(
                    F.when(
                        F.col("sentiment")=="positive",
                        1
                    )
                    .otherwise(0)
                ),
                4
            )
            .alias(
                "positive_review_ratio"
            ),


            F.round(
                F.avg(
                    F.when(
                        F.col("sentiment")=="negative",
                        1
                    )
                    .otherwise(0)
                ),
                4
            )
            .alias(
                "negative_review_ratio"
            ),


            # Restaurant-related complaints

            F.round(
                F.avg(
                    F.when(
                        F.col("issue_food_quality"),
                        1
                    )
                    .otherwise(0)
                ),
                4
            )
            .alias(
                "food_quality_issue_ratio"
            ),


            F.round(
                F.avg(
                    F.when(
                        F.col("issue_pricing"),
                        1
                    )
                    .otherwise(0)
                ),
                4
            )
            .alias(
                "pricing_issue_ratio"
            ),


            F.round(
                F.avg(
                    F.when(
                        F.col("issue_portion_size"),
                        1
                    )
                    .otherwise(0)
                ),
                4
            )
            .alias(
                "portion_issue_ratio"
            )

        )
    )



    # =====================================================
    # 3. Preferred Restaurant
    #
    # NOTE: preference_status is intentionally NOT selected
    # here. After filtering to preference_rank == 1, that
    # column can only ever be "preferred" — it carries no
    # information at this grain. customer_preference_status
    # (built in step 4) is the single status column for this
    # table, so there's no ambiguity about which one to use.
    # =====================================================

    preferred_restaurant = (

        df_pref

        # customer_restaurant_preference owns this logic
        .filter(
            F.col("preference_rank")==1
        )


        .join(

            df_restaurants.select(
                "restaurant_id",
                F.col("name")
                .alias("preferred_restaurant_name")
            ),

            on="restaurant_id",

            how="left"

        )


        .select(

            "customer_id",

            F.col("restaurant_id")
            .alias(
                "preferred_restaurant_id"
            ),


            "preferred_restaurant_name",

            "preference_score",

            "order_share"

        )
    )



    # =====================================================
    # 4. Customer 360 Final Profile
    #
    # FIX: base table is now dim_customer, not customer_orders.
    # Previously, any customer with zero orders — or whose only
    # orders never reached completed/delivered — was silently
    # absent from this table entirely. A "360" profile that
    # drops some customers defeats its own purpose; this way
    # every customer in dim_customer gets a row, with order/
    # review/preference metrics filled in where they exist and
    # left null where they genuinely don't apply yet.
    # =====================================================

    customer_profile = (

        df_customers

        .select("customer_id")

        .join(
            customer_orders,

            on="customer_id",

            how="left"
        )


        .join(
            customer_reviews,

            on="customer_id",

            how="left"
        )


        .join(
            preferred_restaurant,

            on="customer_id",

            how="left"
        )


        .fillna(
            {
                "total_orders": 0,
                "lifetime_spend": 0,
                "total_reviews_given": 0,
                "positive_review_ratio": 0,
                "negative_review_ratio": 0,
                "food_quality_issue_ratio": 0,
                "pricing_issue_ratio": 0,
                "portion_issue_ratio": 0
            }
        )

        # avg_order_value, last_order_date, days_since_last_order,
        # avg_order_hour, weekend_order_ratio, avg_rating_given
        # deliberately NOT filled — "no data yet" is not the same
        # as "zero", same rule used throughout the rest of gold.

        .withColumn(
            "loyalty_segment",
            F.coalesce(
                F.col("loyalty_segment"),
                F.lit("No Orders Yet")
            )
        )


        .withColumn(

            "customer_preference_status",

            F.when(
                F.col("preferred_restaurant_id").isNotNull(),
                "has_preference"
            )

            .when(
                F.col("total_orders") == 0,
                "no_orders_yet"
            )

            .otherwise(
                "insufficient_data"
            )

        )
    )


    return customer_profile