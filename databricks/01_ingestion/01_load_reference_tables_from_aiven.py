"""
databricks/01_ingestion/01_load_reference_tables_from_aiven.py
----------------------------------------------------
Ingests batch reference tables directly from Aiven PostgreSQL into
bronze Delta tables using PySpark JDBC. Credentials are retrieved
from a Databricks secret scope (aiven-postgres), never hardcoded
or stored in cluster-visible plaintext config.
"""

CATALOG = "zaferan_sofreh"
SCHEMA = "bronze"
SECRET_SCOPE = "aiven-postgres"

POSTGRES_HOST     = dbutils.secrets.get(scope=SECRET_SCOPE, key="host")
POSTGRES_PORT     = dbutils.secrets.get(scope=SECRET_SCOPE, key="port")
POSTGRES_DB       = dbutils.secrets.get(scope=SECRET_SCOPE, key="db")
POSTGRES_USER     = dbutils.secrets.get(scope=SECRET_SCOPE, key="user")
POSTGRES_PASSWORD = dbutils.secrets.get(scope=SECRET_SCOPE, key="password")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}?sslmode=require"

# Ensure target catalog and schema exist in Unity Catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

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
    df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

    print(f"Successfully loaded {df.count()} rows -> {full_table_name}")

print("\nAll reference tables ingested successfully into Databricks Bronze Layer!")