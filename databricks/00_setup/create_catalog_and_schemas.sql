-- databricks/00_setup/create_catalog_and_schemas.sql
--DROP CATALOG IF EXISTS zaferan_sofreh CASCADE;
CREATE CATALOG IF NOT EXISTS zaferan_sofreh;

-- Medallion Schemas
CREATE SCHEMA IF NOT EXISTS zaferan_sofreh.bronze;
CREATE SCHEMA IF NOT EXISTS zaferan_sofreh.silver;
CREATE SCHEMA IF NOT EXISTS zaferan_sofreh.gold_v2;

-- Advanced Analytics / Machine Learning Schema (SARIMA & Forecasts)
CREATE SCHEMA IF NOT EXISTS zaferan_sofreh.platinum;

-- System / Operational Schemas
CREATE SCHEMA IF NOT EXISTS zaferan_sofreh._checkpoints;

-- Volume for the Aiven CA certificate (uploaded manually via the
-- Catalog UI or `databricks fs cp ca.pem dbfs:/Volumes/...` after this runs)
CREATE VOLUME IF NOT EXISTS zaferan_sofreh.bronze.kafka_certs;

-- Volume for uploading the CSVs produced by run_pipeline.py, read by
-- 01_ingestion/load_reference_tables.py
CREATE VOLUME IF NOT EXISTS zaferan_sofreh.bronze.landing;

-- Volume for streaming checkpoints — Unity Catalog volumes are the
-- recommended governed storage for this, in place of raw DBFS paths.
CREATE VOLUME IF NOT EXISTS zaferan_sofreh._checkpoints.restaurant_orders_stream;