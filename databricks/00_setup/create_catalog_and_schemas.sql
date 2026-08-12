-- databricks/00_setup/create_catalog_and_schemas.sql
-- -----------------------------------------------------------------------
-- One-time setup. Run this once (as a workspace admin / whoever has
-- CREATE CATALOG rights) before running anything under 01_ingestion/.
--
-- Layout:
--   zaferan_sofreh.bronze        raw landed data, one table per source
--   zaferan_sofreh.silver        cleaned/conformed tables (future work)
--   zaferan_sofreh.gold          aggregates/KPIs (future work)
--   zaferan_sofreh._checkpoints  streaming checkpoints only — kept out of
--                                 bronze so cleaning up data never risks
--                                 wiping checkpoint state you still need
-- -----------------------------------------------------------------------

CREATE CATALOG IF NOT EXISTS zaferan_sofreh;

CREATE SCHEMA IF NOT EXISTS zaferan_sofreh.bronze;
CREATE SCHEMA IF NOT EXISTS zaferan_sofreh.silver;
CREATE SCHEMA IF NOT EXISTS zaferan_sofreh.gold;
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
