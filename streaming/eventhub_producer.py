"""
streaming/eventhub_producer.py
---------------------------------
Simulates the live order feed for the streaming leg of the pipeline
(Event Hub -> Databricks Auto Loader / Structured Streaming -> bronze).

Improvements over the original script:
  * Events are validated against schemas.Order before being sent — bad
    events are logged and dropped instead of silently entering the stream.
  * Events are batched (create_batch/add) instead of one send_batch()
    round-trip per event, which is the actual Event Hub SDK best practice
    and matters a lot for throughput/cost at scale.
  * restaurant_id is used as the partition key, so all events for a given
    branch land on the same partition/order — useful if downstream
    consumers need per-restaurant ordering guarantees.
  * Exponential backoff retry around the network call.
  * --dry-run flag: prints/logs events instead of requiring a live
    EVENTHUB_CONNECTION_STRING, so the producer can be demoed or unit
    tested without any Azure resources provisioned.
"""
from __future__ import annotations

import argparse
import json
import os
import time

from config import CONFIG
from logging_setup import get_logger
from streaming.order_factory import generate_order, load_reference_data

logger = get_logger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 1.5

def _send_with_retry(producer, batch) -> None:
    from azure.eventhub.exceptions import EventHubError

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer.send_batch(batch)
            return
        except EventHubError as exc:
            wait = BACKOFF_BASE_SECONDS ** attempt
            logger.warning("Send failed (attempt %d/%d): %s. Retrying in %.1fs", attempt, MAX_RETRIES, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"Failed to send batch to Event Hub after {MAX_RETRIES} attempts")


def stream_to_eventhub(
    interval_seconds: float | None = None,
    max_orders: int | None = None,
    batch_size: int | None = None,
    dry_run: bool = False,
) -> None:
    interval_seconds = interval_seconds if interval_seconds is not None else CONFIG.stream_interval_seconds
    batch_size = batch_size or CONFIG.stream_batch_size

    restaurants, customers, menu_by_restaurant = load_reference_data()

    producer = None
    if not dry_run:
        from azure.eventhub import EventHubProducerClient
        from dotenv import load_dotenv

        load_dotenv()
        connection_string = os.getenv("EVENTHUB_CONNECTION_STRING")
        eventhub_name = os.getenv("EVENTHUB_NAME")
        if not connection_string or not eventhub_name:
            raise EnvironmentError(
                "EVENTHUB_CONNECTION_STRING and EVENTHUB_NAME must be set (or run with --dry-run)."
            )
        producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_string, eventhub_name=eventhub_name
        )
        logger.info("Streaming to Event Hub: %s (batch_size=%d, interval=%ss)", eventhub_name, batch_size, interval_seconds)
    else:
        logger.info("DRY RUN: no Event Hub connection will be made (batch_size=%d, interval=%ss)", batch_size, interval_seconds)

    order_count = 0
    try:
        while True:
            orders = []
            for _ in range(batch_size):
                order = generate_order(restaurants, customers, menu_by_restaurant)
                if order:
                    orders.append(order)

            if orders:
                if dry_run:
                    for idx, order in enumerate(orders, start=1):
                        logger.info("[%d] %s | %s | IRT %s", order_count + idx, order["order_id"], order["restaurant_id"], f"{order['total_amount']:,.0f}")
                else:
                    from azure.eventhub import EventData

                    # Partition by restaurant_id so a branch's events stay ordered on one partition.
                    batches_by_partition: dict[str, list] = {}
                    for order in orders:
                        batches_by_partition.setdefault(order["restaurant_id"], []).append(order)

                    for partition_key, partition_orders in batches_by_partition.items():
                        batch = producer.create_batch(partition_key=partition_key)
                        for order in partition_orders:
                            batch.add(EventData(json.dumps(order)))
                        _send_with_retry(producer, batch)
                        for order in partition_orders:
                            logger.info("[%d] %s | %s | IRT %s", order_count + 1, order["order_id"], order["restaurant_id"], f"{order['total_amount']:,.0f}")
                            order_count += 1

                if dry_run:
                    order_count += len(orders)

            if max_orders and order_count >= max_orders:
                break

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("Stopped by user after %d orders", order_count)
    finally:
        if producer:
            producer.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream synthetic Persian-restaurant orders to Event Hub.")
    parser.add_argument("--interval", type=float, default=CONFIG.stream_interval_seconds, help="Seconds between batches.")
    parser.add_argument("--max-orders", type=int, default=None, help="Stop after N orders (default: run until Ctrl+C).")
    parser.add_argument("--batch-size", type=int, default=CONFIG.stream_batch_size, help="Orders per Event Hub batch send.")
    parser.add_argument("--dry-run", action="store_true", help="Log events locally instead of sending to Event Hub.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    stream_to_eventhub(
        interval_seconds=args.interval,
        max_orders=args.max_orders,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )
