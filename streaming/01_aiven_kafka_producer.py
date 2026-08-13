"""
streaming/aiven_kafka_producer.py
------------------------------------
Same order stream as eventhub_producer.py, sent to Aiven's managed Kafka
instead of Azure Event Hub. Aiven is a reasonable Event-Hub substitute
when you don't have an Azure subscription: its free tier gives you a
SASL_SSL Kafka endpoint, and Databricks' `kafka` source format speaks
the Kafka wire protocol directly — no Azure-specific connector needed
on the read side either (see databricks/stream_orders_from_aiven.py).

Credentials are read from environment variables — never hardcode a
broker password in the script itself. Put them in a local .env
(gitignored) or your platform's secret manager:

    AIVEN_BOOTSTRAP_SERVERS=kafka-xxxx.aivencloud.com:PORT
    AIVEN_USERNAME=avnadmin
    AIVEN_PASSWORD=...
    AIVEN_TOPIC=restaurant-orders
    AIVEN_CA_CERT_PATH=./certs/ca.pem   # optional, see note below

Usage:
    python -m streaming.aiven_kafka_producer --dry-run --interval 2
    python -m streaming.aiven_kafka_producer --interval 3 --max-orders 100
"""
from __future__ import annotations

import argparse
import json
import os
import ssl
import time

from config import CONFIG
from logging_setup import get_logger
from streaming.order_factory import generate_order, load_reference_data

logger = get_logger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 1.5

def _build_producer():
    """Creates a kafka-python KafkaProducer configured for Aiven's SASL_SSL endpoint."""
    from dotenv import load_dotenv
    from kafka import KafkaProducer

    load_dotenv()

    bootstrap_servers = os.getenv("AIVEN_BOOTSTRAP_SERVERS")
    username = os.getenv("AIVEN_USERNAME")
    password = os.getenv("AIVEN_PASSWORD")

    if not all([bootstrap_servers, username, password]):
        raise EnvironmentError(
            "AIVEN_BOOTSTRAP_SERVERS, AIVEN_USERNAME and AIVEN_PASSWORD must be set "
            "(or run with --dry-run)."
        )

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=username,
        sasl_plain_password=password,
        ssl_context=ssl_context,
        api_version=(2, 5, 0),  # <--- ADD THIS LINE TO BYPASS API VERSION PROBING
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )

def _send_with_retry(producer, topic: str, key: str, value: dict) -> None:
    from kafka.errors import KafkaError

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            future = producer.send(topic, key=key, value=value)
            future.get(timeout=10)  # blocks until ack or raises
            return
        except KafkaError as exc:
            wait = BACKOFF_BASE_SECONDS ** attempt
            logger.warning("Send failed (attempt %d/%d): %s. Retrying in %.1fs", attempt, MAX_RETRIES, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"Failed to send message to Aiven Kafka after {MAX_RETRIES} attempts")


def stream_to_aiven(
    interval_seconds: float | None = None,
    max_orders: int | None = None,
    dry_run: bool = False,
) -> None:
    interval_seconds = interval_seconds if interval_seconds is not None else CONFIG.stream_interval_seconds
    topic = os.getenv("AIVEN_TOPIC", "restaurant-orders")

    restaurants, customers, menu_by_restaurant = load_reference_data()

    producer = None
    if not dry_run:
        producer = _build_producer()
        logger.info("Streaming to Aiven Kafka topic '%s' (interval=%ss)", topic, interval_seconds)
    else:
        logger.info("DRY RUN: no Kafka connection will be made (topic='%s', interval=%ss)", topic, interval_seconds)

    order_count = 0
    try:
        while True:
            order = generate_order(restaurants, customers, menu_by_restaurant)
            if order:
                if dry_run:
                    logger.info(
                        "[%d] %s | %s | IRT %s",
                        order_count + 1, order["order_id"], order["restaurant_id"], f"{order['total_amount']:,.0f}",
                    )
                else:
                    # restaurant_id as the message key -> same restaurant's orders
                    # land on the same partition, preserving per-branch order.
                    _send_with_retry(producer, topic, key=order["restaurant_id"], value=order)
                    logger.info(
                        "[%d] %s | %s | IRT %s -> topic '%s'",
                        order_count + 1, order["order_id"], order["restaurant_id"],
                        f"{order['total_amount']:,.0f}", topic,
                    )
                order_count += 1

            if max_orders and order_count >= max_orders:
                break

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("Stopped by user after %d orders", order_count)
    finally:
        if producer:
            producer.flush()
            producer.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream synthetic Persian-restaurant orders to Aiven Kafka.")
    parser.add_argument("--interval", type=float, default=CONFIG.stream_interval_seconds, help="Seconds between messages.")
    parser.add_argument("--max-orders", type=int, default=None, help="Stop after N orders (default: run until Ctrl+C).")
    parser.add_argument("--dry-run", action="store_true", help="Log events locally instead of sending to Aiven.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    stream_to_aiven(
        interval_seconds=args.interval,
        max_orders=args.max_orders,
        dry_run=args.dry_run,
    )
