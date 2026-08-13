"""
databricks/02_transform/build_orders_unified_silver.py
-----------------------------------------------------------
Silver layer: combines the historical (batch-loaded) and streaming
(Kafka-ingested) order sources into one conformed table.

Why this is a *batch* table, not a chained streaming read:
Unioning a static source into a live streaming query causes Spark to
re-scan and re-emit the static rows on every micro-batch trigger,
producing unbounded duplicates over time. So instead of chaining off
the streaming bronze table with dp.read_stream, this table reads both
bronze tables as static snapshots (spark.table) and is recomputed each
time the pipeline runs — a normal materialized batch table, just fed
by two sources instead of one.

If you'd rather have a single ever-growing table that consumers never
have to union themselves, see the "seed-then-stream" note at the
bottom of this file — a different, also-valid pattern with a different
trade-off.
"""
from pyspark import pipelines as dp
from pyspark.sql.functions import lit, to_timestamp

# Columns present in both sources, in the target order. Kafka-specific
# columns (kafka_key, raw_json_payload, partition, offset, kafka_timestamp,
# ingested_at) and batch-load-specific columns (_ingested_at, _source_file)
# are intentionally dropped here — they're lineage metadata for their own
# bronze table, not part of the conformed order record.
COMMON_COLUMNS = [
    "order_id", "timestamp", "restaurant_id", "customer_id", "order_type",
    "items", "total_amount", "payment_method", "order_status", "created_at",
]


@dp.table(
    name="orders_unified_silver",
    comment="Historical (batch) + streaming (Kafka) orders, conformed to one schema",
)
def orders_unified_silver():
    historical = (
        spark.table("zaferan_sofreh.bronze.historical_orders_raw")
        .select(*COMMON_COLUMNS)
        .withColumn("source_system", lit("historical_batch"))
    )

    streaming = (
        spark.table("zaferan_sofreh.bronze.restaurant_orders_stream_bronze")
        .select(*COMMON_COLUMNS)
        .withColumn("source_system", lit("streaming_kafka"))
    )

    unified = historical.unionByName(streaming, allowMissingColumns=True)

    # Both sources store timestamp/created_at as ISO strings — cast once
    # here so every downstream consumer gets a real TimestampType instead
    # of re-parsing a string in every query that touches this table.
    return (
        unified
        .withColumn("timestamp", to_timestamp("timestamp"))
        .withColumn("created_at", to_timestamp("created_at"))
    )


# ------------------------------------------------------------------------
# Alternative pattern: seed-then-stream
#
# If you'd rather have ONE table that just keeps growing — instead of a
# union recomputed on every pipeline run — seed it once:
#   1. Reshape historical_orders_raw's rows to match
#      restaurant_orders_stream_bronze's exact schema (NULL out the
#      Kafka-only columns: kafka_key, raw_json_payload, partition, offset,
#      kafka_timestamp).
#   2. Write that reshaped batch into restaurant_orders_stream_bronze's
#      table location once, with mode="append", *before* the stream first
#      runs.
#   3. From then on, only Kafka writes to it. No downstream query ever
#      needs to union anything.
#
# Trade-off: simpler for every consumer downstream, but the backfill is a
# manual one-shot step you have to remember and sequence correctly, rather
# than something idempotently recomputed by the pipeline every run. For a
# resume project where you want to show "I know how to reconcile
# historical + streaming sources," the union approach above is the more
# instructive one to build.
# ------------------------------------------------------------------------
