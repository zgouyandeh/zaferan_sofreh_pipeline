"""
databricks/stream_orders_from_aiven.py
-----------------------------------------
Databricks notebook/job: reads the restaurant-order stream from Aiven
Kafka and lands it as a bronze Delta table with a checkpoint, instead of
the memory sink used in the sample project (memory sinks are for
notebook debugging only — they hold no data once the cluster stops, so
nothing downstream could ever depend on them).

Run this as a Databricks notebook or job; it relies on `spark` and
`dbutils`, which only exist inside a Databricks runtime.

Prerequisites:
  1. Run databricks/00_setup/create_catalog_and_schemas.sql once, first.
  2. A Databricks secret scope holding the Aiven credentials:
         databricks secrets create-scope aiven
         databricks secrets put-secret aiven kafka-username
         databricks secrets put-secret aiven kafka-password
  3. The Aiven CA certificate (ca.pem, from the Aiven console) uploaded
     to /Volumes/zaferan_sofreh/bronze/kafka_certs/ca.pem
  4. streaming/aiven_kafka_producer.py running somewhere and publishing
     to the same AIVEN_TOPIC this reads from.
"""
import time

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ==============================================================================
# 1. Connection details — pulled from a Databricks secret scope, never hardcoded.
#    (The sample project's send_to_aiven.py had the broker password inline in
#    plaintext — don't carry that into anything checked into a repo.)
# ==============================================================================
AIVEN_BOOTSTRAP = "kafka-26f2fbb0-mrs-224b.e.aivencloud.com:17355"  # replace with your own
AIVEN_USER = dbutils.secrets.get(scope="aiven", key="kafka-username")
AIVEN_PASSWORD = dbutils.secrets.get(scope="aiven", key="kafka-password")
TOPIC_NAME = "restaurant-orders"

CA_CERT_PATH = "/Volumes/zaferan_sofreh/bronze/kafka_certs/ca.pem"

CATALOG = "zaferan_sofreh"
SCHEMA = "bronze"
TABLE_NAME = f"{CATALOG}.{SCHEMA}.restaurant_orders_stream_raw"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/_checkpoints/restaurant_orders_stream"

jaas_config = (
    f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
    f'username="{AIVEN_USER}" password="{AIVEN_PASSWORD}";'
)

# ==============================================================================
# 2. Read stream from Aiven Kafka
# ==============================================================================
raw_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", AIVEN_BOOTSTRAP)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("kafka.ssl.truststore.type", "PEM")
    .option("kafka.ssl.truststore.location", CA_CERT_PATH)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)  # backpressure guard for availableNow batches
    .load()
)

# ==============================================================================
# 3. Schema — mirrors schemas.Order in the generator, including the nested
#    items array. This is the real "data contract" boundary: whatever the
#    producer emits, this is what bronze is willing to accept as well-formed.
# ==============================================================================
order_item_schema = StructType(
    [
        StructField("item_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("subtotal", DoubleType(), True),
    ]
)

order_schema = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("restaurant_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_type", StringType(), True),
        StructField("items", ArrayType(order_item_schema), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)

parsed_stream_df = (
    raw_stream_df.selectExpr(
        "CAST(key AS STRING) as kafka_key",
        "CAST(value AS STRING) as json_payload",
        "timestamp",
        "partition",
        "offset",
    )
    .select(
        col("kafka_key"),
        from_json(col("json_payload"), order_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition"),
        col("offset"),
    )
    .select("kafka_key", "data.*", "kafka_timestamp", "partition", "offset")
)

# ==============================================================================
# 4. Write to a bronze Delta table with checkpointing (not the memory sink —
#    memory tables vanish when the cluster stops, so nothing durable ever
#    lands). `availableNow` processes everything currently in the topic and
#    then stops, which suits a scheduled Databricks Job; switch to
#    `.trigger(processingTime="30 seconds")` for an always-on job cluster.
# ==============================================================================
query = (
    parsed_stream_df.writeStream.format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .toTable(TABLE_NAME)
)

query.awaitTermination()

print(f"Batch complete. Row count in {TABLE_NAME}:")
display(spark.table(TABLE_NAME).count())
