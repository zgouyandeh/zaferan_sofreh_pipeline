"""
databricks/03_marts/01_daily_sales_summery.py
------------------------------------------------------  
This code reads the silver fact_orders table, applies aggregations and quality checks, 
and writes the result to a daily_sales_summary materialized view.
It uses HyperLogLog (HLL) sketches for approximate distinct counting of customers,
which is useful for large datasets where exact counts are not necessary and can save memory and computation time.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="daily_sales_summary",   
    partition_cols=["order_date"],
    table_properties={"quality": "gold"},
    comment="Gold daily aggregations with HyperLogLog customer sketches",
)
def daily_sales_summary():
    return (
        dp.read("zaferan_sofreh.silver.fact_orders")  
        .filter(F.col("order_status").isin("completed", "delivered"))
        .groupBy("order_date")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").cast("decimal(12,2)").alias("total_revenue"),
            F.expr("hll_sketch_agg(customer_id)").alias("customer_hll_sketch"),
            F.countDistinct("restaurant_id").alias("unique_restaurants"),
            F.count(F.when(F.col("order_type") == "dine_in", F.col("order_id"))).alias("dine_in_orders"),
            F.count(F.when(F.col("order_type") == "takeaway", F.col("order_id"))).alias("takeaway_orders"),
            F.count(F.when(F.col("order_type") == "delivery", F.col("order_id"))).alias("delivery_orders"),
        )
    )