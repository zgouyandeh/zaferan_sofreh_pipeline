"""
databricks/01_ingestion/load_reference_tables_from_aiven.py
----------------------------------------------------
Ingests batch reference tables directly from Aiven PostgreSQL into 
bronze Delta tables using PySpark JDBC with configurations retrieved
from Spark Session environment variables.
"""

CATALOG = "zaferan_sofreh"
SCHEMA = "bronze"

# Retrieve connection settings dynamically from Cluster Spark Config
POSTGRES_HOST = spark.conf.get("spark.aiven.postgres.host")
POSTGRES_PORT = spark.conf.get("spark.aiven.postgres.port", "17342")
POSTGRES_DB = spark.conf.get("spark.aiven.postgres.db", "defaultdb")
POSTGRES_USER = spark.conf.get("spark.aiven.postgres.user", "avnadmin")
POSTGRES_PASSWORD = spark.conf.get("spark.aiven.postgres.password")

# Construct JDBC Connection URL
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}?sslmode=require"

# Map: (Aiven PostgreSQL Table Name -> Target Delta Table Name)
SOURCES = [
    ("restaurants", "restaurants_raw"),
    ("customers", "customers_raw"),
    ("menu_items", "menu_items_raw"),
    ("historical_orders", "historical_orders_raw"),
    ("customer_reviews", "customer_reviews_raw"),
]

for pg_table, delta_table in SOURCES:
    print(f"Reading '{pg_table}' from Aiven PostgreSQL...")

    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", pg_table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    full_table_name = f"{CATALOG}.{SCHEMA}.{delta_table}"

    # Overwrite Delta table with fresh batch data
    df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

    print(f" Successfully loaded {df.count()} rows -> {full_table_name}")

print("\n All reference tables ingested successfully into Databricks Bronze Layer!")