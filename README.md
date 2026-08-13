# Zaferan Sofreh — Synthetic Data Generator for a Lakehouse Pipeline

Synthetic-data generator for a fictional Persian-cuisine restaurant chain
("Zaferan Sofreh" — زعفران سفره, *"Saffron Table"*), producing both a
**batch/historical source** and a **live streaming source**, feeding a
bronze → silver → gold lakehouse pipeline (e.g. Databricks Auto Loader +
Lakeflow Declarative Pipelines).

## Architecture

```
                         ┌────────────────────┐
   Step 1 (batch)  ───▶  │ reference_data.py   │──▶ restaurants.csv
                         │ (dimension tables)   │──▶ menu_items.csv
                         └────────────────────┘──▶ customers.csv
                                    │
                                    ▼
   Step 2 (batch)  ───▶  historical_orders.py ──▶ historical_orders.csv
                                    │
                                    ▼
   Step 3 (batch)  ───▶  reviews.py            ──▶ customer_reviews.csv

   Live feed       ───▶  eventhub_producer.py  ──▶ Azure Event Hub
                         (--dry-run works with no Azure resources)
```

Every order/review record is validated against a Pydantic **data
contract** (`schemas.py`) before it's written — a stand-in for the
schema enforcement Auto Loader / Great Expectations would do on
ingestion into the real bronze layer. Malformed records are logged and
dropped rather than silently persisted.

## Quickstart

```bash
pip install -r requirements.txt

# Batch leg: reference data -> historical orders -> reviews
python run_pipeline.py --orders 8000 --months-back 6 --review-rate 0.35

# Streaming leg (aiven.io)
python -m streaming.aiven_kafka_producer

# Streaming leg (real Event Hub — set EVENTHUB_CONNECTION_STRING /
# EVENTHUB_NAME in a .env file first):
python -m streaming.eventhub_producer --interval 3 --batch-size 5
```

Run the streaming module with `-m` (not as a bare script) so its
package-relative imports resolve — this is standard for any
multi-file Python package, not specific to this project.

## Streaming transport: Event Hub or Aiven Kafka

The order-generation logic (`streaming/order_factory.py`) is transport-agnostic
— it produces the same schema-validated payload either way. Two producers
consume it:

| | `streaming/eventhub_producer.py` | `streaming/aiven_kafka_producer.py` |
|---|---|---|
| Broker | Azure Event Hub | Aiven managed Kafka (free tier) |
| SDK | `azure-eventhub` | `kafka-python` |
| Auth | connection string | SASL_SSL / SCRAM-SHA-256 |
| Ordering key | `restaurant_id` as partition key | `restaurant_id` as message key |

Use Aiven when you don't have an Azure subscription — its free tier exposes a
standard Kafka wire-protocol endpoint, so `databricks/stream_orders_from_aiven.py`
reads it with Spark's built-in `kafka` source, no Azure-specific connector
needed. Copy `.env.example` to `.env` and fill in the `AIVEN_*` values (get
`AIVEN_CA_CERT_PATH`'s `ca.pem` from the Aiven console).

```bash
python -m streaming.aiven_kafka_producer --interval 3
```

On the Databricks side, run `databricks/01_ingestion/stream_orders_from_aiven.py`
as a notebook or job (after running `databricks/00_setup/create_catalog_and_schemas.sql`
once). Compared to a typical quick-test version of this script,
it: parses the *actual* nested order schema (items array, totals, etc.)
instead of a 3-field flat payload; pulls the Kafka password from a
Databricks secret scope instead of a hardcoded string; and writes to a
checkpointed bronze Delta table instead of a `memory` sink — a memory
sink holds no data once the cluster stops, so nothing downstream can
depend on it.

## What changed from the original scripts, and why

| Area | Before | After | Why it matters |
|---|---|---|---|
| Config | Hardcoded record counts, relative paths | `config.py`, env-var overridable dataclass | Same code runs unchanged in local dev, CI, or a scheduled job |
| Logging | `print()` | `logging` module, timestamped/leveled | Redirectable to a log aggregator; filterable by severity |
| Data contracts | None — malformed rows would flow silently | `schemas.py` (Pydantic): validates totals, enums, ranges | Mirrors schema-on-write / data-quality gates in a real bronze layer |
| Streaming throughput | 1 `send_batch()` call per event | Events batched via `create_batch()`/`add()`, partitioned by `restaurant_id` | Matches Event Hub SDK best practice; partition key gives per-branch ordering |
| Streaming reliability | No retry logic | Exponential backoff around `send_batch` | Network calls fail; this is table stakes for a production producer |
| Local testability | Streaming script *required* live Azure credentials | `--dry-run` flag | Lets you demo/test the producer with zero cloud spend |
| Package structure | Flat scripts numbered `00_`–`04_`, imported via `importlib` | Proper `generators/` and `streaming/` packages with `run_pipeline.py` orchestrator | Standard, `pip install -e .`-able layout; no numeric-prefixed module hacks |
| Domain data | Indian dishes/names (`Faker('en_IN')`) generated by an unrelated Indian-restaurant chain in the UAE | Authentic Persian menu (Ghormeh Sabzi, Fesenjan, Tahchin, etc.), Iranian cities (Tehran, Isfahan, Shiraz, Mashhad, Tabriz, Karaj), Iranian customer names, Toman pricing | Matches the theme you wanted and is internally consistent (cuisine ↔ names ↔ geography ↔ currency) |

## Known limitations (worth naming, not hiding, on a resume project)

- Reviews are template-based, not LLM-generated — good enough for
  volume/rating-distribution testing, not for NLP/sentiment-analysis
  demos. If you want the latter, swap `_generate_review_text` for a
  call to an LLM API.
- No uniqueness constraint enforcement across generator runs (re-running
  `run_pipeline.py` will produce a fresh, non-overlapping dataset each
  time — set `--seed` for reproducibility instead).
- `total_amount` doesn't include tax/service charge — add a
  `TAX_RATE` constant in `config.py` if your gold-layer KPIs need it.

## Suggested resume bullets

Adapt to what you actually build on top of this (the generator is the
*source*, not the pipeline itself — the bullets below assume you carry
this into bronze/silver/gold, which is the natural next step):

- Designed and built a synthetic multi-source data generator (batch +
  streaming) simulating a multi-branch restaurant chain's OLTP and
  event-stream data, with Pydantic-enforced schema contracts to model
  realistic bronze-layer data-quality gating.
- Implemented a streaming producer for Azure Event Hub with batched
  sends, partition-key routing, and retry/backoff, plus a dry-run mode
  enabling local development and testing without provisioned cloud
  resources.
- Built a configuration-driven, testable Python package (dataclass
  config, structured logging, CLI entrypoints) replacing a set of
  hardcoded standalone scripts — supporting reproducible runs via a
  fixed random seed.
"# zaferan_sofreh_pipeline" 
