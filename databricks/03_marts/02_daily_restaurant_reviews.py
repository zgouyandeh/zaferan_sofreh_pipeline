from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

@dp.materialized_view(
    name="daily_restaurant_reviews",   
    table_properties={"quality": "gold"},
    comment="Gold layer aggregates with date-based overwrites",
)


def daily_restaurant_reviews():
    df_review_status=(
        dp.read("zaferan_sofreh.silver.fact_review").
        groupBy("restaurant_id")
        .agg(
            countDistinct("review_id").alias("total_reviews"),
            round(avg("rating"),2).alias("avg_rating"),
            sum(when(col("rating") == 5, 1).otherwise(0)).alias("rating_5_stars"),
            sum(when(col("rating") == 4, 1).otherwise(0)).alias("rating_4_stars"),
            sum(when(col("rating") == 3, 1).otherwise(0)).alias("rating_3_stars"),
            sum(when(col("rating") == 2, 1).otherwise(0)).alias("rating_2_stars"),
            sum(when(col("rating") == 1, 1).otherwise(0)).alias("rating_1_stars"),
            sum(when(col("sentiment") == "positive", 1).otherwise(0)).alias("sentiment_positive_review"),
            sum(when(col("sentiment") == "negative", 1).otherwise(0)).alias("sentiment_negative_review"),
            sum(when(col("sentiment") == "neutral", 1).otherwise(0)).alias("sentiment_neutral_review") 
        )
    )
    
    df_restaurant=dp.read("zaferan_sofreh.silver.dim_restaurant")

    df_restaurant_reviews=(
        df_restaurant.join(df_review_status, on="restaurant_id", how="left")
        .select(
            "restaurant_id",
            "city",
            coalesce(col("total_reviews"),lit(0)).alias("total_reviews"),
            coalesce(col("avg_rating"),lit(0)).alias("avg_rating"),
            coalesce(col("rating_5_stars"),lit(0)).alias("rating_5_stars"),
            
            coalesce(col("rating_4_stars"),lit(0)).alias("rating_4_stars"),
            coalesce(col("rating_3_stars"),lit(0)).alias("rating_3_stars"),
            coalesce(col("rating_2_stars"),lit(0)).alias("rating_2_stars"),
            coalesce(col("rating_1_stars"),lit(0)).alias("rating_1_stars"),
            coalesce(col("sentiment_positive_review"),lit(0)).alias("sentiment_positive_review"),
            coalesce(col("sentiment_negative_review"),lit(0)).alias("sentiment_negative_review"),
            coalesce(col("sentiment_neutral_review"),lit(0)).alias("sentiment_neutral_review")
        )
    )
    return df_restaurant_reviews

