"""
databricks/01_ingestion/stream_orders_from_aiven.py
------------------------------------------------------
Lakeflow Declarative Pipeline: reads the restaurant-order stream from
Aiven Kafka and materializes a bronze streaming table. The pipeline
engine manages checkpointing and incremental writes automatically —
no manual .option("checkpointLocation", ...) needed, unlike a plain
Structured Streaming job.

Design notes (see project README for the fuller discussion):
  * `items` is intentionally kept as StringType here, not parsed into
    ArrayType(order_item_schema). Bronze stays schema-flexible; the
    nested structure gets parsed and validated in silver, where a
    breaking upstream change won't fail ingestion. `order_item_schema`
    is defined here so silver can import and reuse it.
  * `raw_json_payload` is preserved verbatim alongside the parsed
    columns. If from_json() can't parse a record, the parsed columns
    resolve to NULL, but the original bytes are never lost — you can
    always replay/reprocess from bronze.
  * `ingested_at` records when Databricks processed the record,
    separate from `kafka_timestamp` (when Aiven received it) — useful
    for diagnosing pipeline lag.
  * De-duplication (Kafka is at-least-once, so retries can produce
    duplicate order_ids) is deliberately NOT handled here. Bronze is
    append-only and expected to contain duplicates; dedupe on
    `order_id` when building the silver table instead.

Prerequisites:
  1. Run databricks/00_setup/create_catalog_and_schemas.sql once.
  2. A Databricks secret scope holding the Aiven credentials:
         databricks secrets create-scope aiven
         databricks secrets put-secret aiven kafka-username
         databricks secrets put-secret aiven kafka-password
  3. The Aiven CA certificate (ca.pem, from the Aiven console) uploaded
     to /Volumes/zaferan_sofreh/bronze/kafka_certs/ca.pem
  4. streaming/aiven_kafka_producer.py running somewhere and publishing
     to the same AIVEN_TOPIC this reads from.
"""
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ==============================================================================
# Connection details — never hardcode credentials in the script itself.
# ==============================================================================
AIVEN_BOOTSTRAP = "kafka-26f2fbb0-mrs-224b.e.aivencloud.com:17355"  
AIVEN_USER = spark.conf.get("aiven.username")
AIVEN_PASSWORD = spark.conf.get("aiven.password")
TOPIC_NAME = "restaurant-orders"
CA_CERT_PATH = "/Volumes/zaferan_sofreh/bronze/kafka_certs/ca.pem"

jaas_config = (
    f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
    f'username="{AIVEN_USER}" password="{AIVEN_PASSWORD}";'
)

# ==============================================================================
# Schema definitions
# ==============================================================================
# Kept for silver's use when it parses `items` out of the raw string —
# not applied to `items` here (see design note above).
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
        StructField("items", StringType(), True),  
        StructField("total_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)

# ==============================================================================
# Streaming table definition
# ==============================================================================
@dp.table(
    name="restaurant_orders_stream_bronze",
    comment="Bronze streaming table: raw restaurant orders from Aiven Kafka",
)
def restaurant_orders_stream_bronze():
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
        .option("maxOffsetsPerTrigger", 10000)
        .option("failOnDataLoss", "false")
        .load()
    )

    return (
        raw_stream_df.selectExpr(
            "CAST(key AS STRING) as kafka_key",
            "CAST(value AS STRING) as raw_json_payload",
            "timestamp",
            "partition",
            "offset",
        )
        .select(
            col("kafka_key"),
            col("raw_json_payload"),  
            from_json(col("raw_json_payload"), order_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset"),
        )
        .select(
            "kafka_key",
            "raw_json_payload",
            "data.*",
            "kafka_timestamp",
            "partition",
            "offset",
        )
        .withColumn("ingested_at", current_timestamp())
    )
