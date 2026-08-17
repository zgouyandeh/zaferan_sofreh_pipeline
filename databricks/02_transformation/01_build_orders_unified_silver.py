import dlt
from pyspark.sql.functions import col, lit, to_timestamp

COMMON_COLUMNS = [
    "order_id", "timestamp", "restaurant_id", "customer_id", "order_type",
    "items", "total_amount", "payment_method", "order_status", "created_at",
]

@dlt.table(
    name="orders_unified_silver",
    comment="Conformed and deduplicated orders from historical and streaming sources",
)
def orders_unified_silver():
    historical = (
        dlt.read("zaferan_sofreh.bronze.historical_orders_raw")
        .select(*COMMON_COLUMNS)
        .withColumn("source_system", lit("historical_batch"))
    )

    streaming = (
        dlt.read("zaferan_sofreh.bronze.restaurant_orders_stream_bronze")
        .select(*COMMON_COLUMNS)
        .withColumn("source_system", lit("streaming_kafka"))
    )

    return (
        historical.unionByName(streaming)
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .withColumn("created_at", to_timestamp(col("created_at")))
        .dropDuplicates(["order_id"])
    )