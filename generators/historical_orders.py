"""
generators/historical_orders.py
---------------------------------
Generates a batch of historical, already-completed orders — this
simulates a one-time backfill / historical load from the OLTP source
system, as opposed to the live stream produced by
streaming/eventhub_producer.py.

Every record is validated against schemas.Order before being written,
so malformed rows are caught at generation time rather than silently
flowing downstream.
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta

import pandas as pd
from pydantic import ValidationError

from config import CONFIG
from logging_setup import get_logger
from schemas import Order

logger = get_logger(__name__)

random.seed(CONFIG.random_seed)

ORDER_TYPES = ["dine_in", "takeaway", "delivery"]
PAYMENT_METHODS = ["cash", "card", "wallet"]
ORDER_STATUSES = ["delivered", "completed"]  # terminal states only for historical data


def _load_reference_data():
    df_restaurants = pd.read_csv(CONFIG.restaurants_path)
    df_customers = pd.read_csv(CONFIG.customers_path)
    df_menu_items = pd.read_csv(CONFIG.menu_items_path)

    restaurants = df_restaurants["restaurant_id"].tolist()
    customers = df_customers["customer_id"].tolist()
    menu_by_restaurant = (
        df_menu_items.groupby("restaurant_id").apply(lambda x: x.to_dict("records")).to_dict()
    )
    return restaurants, customers, menu_by_restaurant


def _build_order_payload(order_date: datetime, restaurant_id: str, customer_id: str, menu_items: list) -> dict:
    num_items = random.randint(1, min(5, len(menu_items)))
    selected_items = random.sample(menu_items, num_items)

    items = []
    total_amount = 0.0
    for item in selected_items:
        quantity = random.randint(1, 3)
        subtotal = round(item["price"] * quantity, 2)
        total_amount += subtotal
        items.append(
            {
                "item_id": item["item_id"],
                "name": item["name"],
                "category": item["category"],
                "quantity": quantity,
                "unit_price": item["price"],
                "subtotal": subtotal,
            }
        )

    order_id = f"ORD-{order_date.strftime('%Y%m%d')}-{random.randint(100000, 999999)}"

    return {
        "order_id": order_id,
        "timestamp": order_date.isoformat(),
        "restaurant_id": restaurant_id,
        "customer_id": customer_id,
        "order_type": random.choice(ORDER_TYPES),
        "items": items,
        "total_amount": round(total_amount, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_status": random.choice(ORDER_STATUSES),
        "created_at": order_date.isoformat(),
    }


def generate_historical_orders(
    num_orders: int | None = None,
    months_back: int | None = None,
) -> pd.DataFrame:
    num_orders = num_orders or CONFIG.n_historical_orders
    months_back = months_back or CONFIG.historical_months_back

    restaurants, customers, menu_by_restaurant = _load_reference_data()

    end_date = datetime.now()
    start_date = end_date - timedelta(days=months_back * 30)

    logger.info("Generating %d orders from %s to %s", num_orders, start_date.date(), end_date.date())

    rows = []
    rejected = 0

    for i in range(num_orders):
        days_offset = random.randint(0, (end_date - start_date).days)
        order_date = start_date + timedelta(days=days_offset)
        order_date = order_date.replace(
            hour=random.randint(10, 22),
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
        )

        restaurant_id = random.choice(restaurants)
        customer_id = random.choice(customers)
        menu_items = menu_by_restaurant[restaurant_id]

        payload = _build_order_payload(order_date, restaurant_id, customer_id, menu_items)

        try:
            validated = Order.model_validate(payload)
        except ValidationError as exc:
            rejected += 1
            logger.warning("Rejected malformed order %s: %s", payload.get("order_id"), exc)
            continue

        rows.append(
            {
                "order_id": validated.order_id,
                "timestamp": validated.timestamp.isoformat(),
                "restaurant_id": validated.restaurant_id,
                "customer_id": validated.customer_id,
                "order_type": validated.order_type.value,
                "items": json.dumps(payload["items"]),  # kept as JSON string for the flat CSV sink
                "total_amount": validated.total_amount,
                "payment_method": validated.payment_method.value,
                "order_status": validated.order_status.value,
                "created_at": validated.created_at.isoformat(),
            }
        )

        if (i + 1) % 1000 == 0:
            logger.info("Generated %d/%d orders...", i + 1, num_orders)

    df_orders = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)

    CONFIG.ensure_dirs()
    df_orders.to_csv(CONFIG.historical_orders_path, index=False)

    logger.info("Wrote historical_orders.csv -> %d rows (%d rejected by schema)", len(df_orders), rejected)
    logger.info(
        "Date range: %s to %s | Total revenue: IRT %s",
        df_orders["timestamp"].min(),
        df_orders["timestamp"].max(),
        f"{df_orders['total_amount'].sum():,.0f}",
    )
    return df_orders


if __name__ == "__main__":
    generate_historical_orders()
