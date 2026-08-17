from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

@dp.materialized_view(
    name="daily_sales_summary",   
    partition_cols=["order_date"],
    table_properties={"quality": "gold"},
    comment="Gold layer aggregates with date-based overwrites",
)
def daily_sales_summery():
    df_daily_sales = (
        spark.read.table("zaferan_sofreh.silver.fact_orders")
        .groupBy("order_date")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("total_amount").cast("decimal(12,2)").alias("total_revenue"),
            avg("total_amount").cast("decimal(10,2)").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("restaurant_id").alias("unique_restaurants"),
            countDistinct(
                when(col("order_type") == "dine_in", col("order_id")).otherwise(None)
            ).alias("dine_in_order"),
            countDistinct(
                when(col("order_type") == "takeaway", col("order_id")).otherwise(None)
            ).alias("takeaway_order"),
            countDistinct(
                when(col("order_type") == "delivery", col("order_id")).otherwise(None)
            ).alias("delivery_order"),
        )
        .select(
            "order_date",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "unique_customers",
            "unique_restaurants",
            "dine_in_order",
            "takeaway_order",
            "delivery_order",
        )
    )
    return df_daily_sales