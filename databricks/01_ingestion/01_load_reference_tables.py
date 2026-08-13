"""
databricks/01_ingestion/load_reference_tables.py
----------------------------------------------------
Loads the batch outputs of run_pipeline.py (restaurants, customers,
menu_items, historical_orders, customer_reviews) into bronze Delta
tables, as plain batch reads — separate from the streaming ingestion
in stream_orders_from_aiven.py, which is a continuous/triggered job.

Run this after:
  1. databricks/00_setup/create_catalog_and_schemas.sql
  2. Uploading the CSVs from your local data/ folder to the landing
     volume, e.g.:
         databricks fs cp -r data/ dbfs:/Volumes/zaferan_sofreh/bronze/landing/
"""
from pyspark.sql.functions import current_timestamp, col

CATALOG = "zaferan_sofreh"
SCHEMA = "bronze"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

# (source_csv_filename, target_table_name)
SOURCES = [
    ("restaurants.csv", "restaurants_raw"),
    ("customers.csv", "customers_raw"),
    ("menu_items.csv", "menu_items_raw"),
    ("historical_orders.csv", "historical_orders_raw"),
    ("customer_reviews.csv", "customer_reviews_raw"),
]

for filename, table_name in SOURCES:
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{LANDING_PATH}/{filename}")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)

    print(f"Loaded {df.count()} rows -> {full_table_name}")
