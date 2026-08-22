"""
databricks/01_ingestion/02_stream_orders_from_aiven.py
------------------------------------------------------
This code reads the restaurant-order stream from
Aiven Kafka and materializes a bronze streaming table. The pipeline
engine manages checkpointing and incremental writes automatically.

Credentials are retrieved securely using Databricks Secrets (dbutils.secrets.get).
The SSL certificate is stored at (/Volumes/zaferan_sofreh/bronze/kafka_certs/ca.pem).
"""

import dlt
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ==============================================================================
# Credentials & Configuration (Retrieved via Databricks Secret Scope)
# ==============================================================================
SECRET_SCOPE = "aiven"

AIVEN_BOOTSTRAP = "kafka-26f2fbb0-mrs-224b.e.aivencloud.com:17355"
AIVEN_USER = dbutils.secrets.get(scope=SECRET_SCOPE, key="kafka-username")
AIVEN_PASSWORD = dbutils.secrets.get(scope=SECRET_SCOPE, key="kafka-password")

TOPIC_NAME = "restaurant-orders"
CA_CERT_PATH = "/Volumes/zaferan_sofreh/bronze/kafka_certs/ca.pem"

jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
    f'username="{AIVEN_USER}" password="{AIVEN_PASSWORD}";'
)

# ==============================================================================
# Schema Definitions
# ==============================================================================
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
# Streaming Table Definition
# ==============================================================================
@dlt.table(
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