"""
streaming/order_factory.py
-----------------------------
Order generation logic shared by every streaming transport (Event Hub,
Aiven Kafka, ...). Keeping this in one place means the schema-validated
payload is identical regardless of which broker it's sent to — the
transport is a swappable detail, the data contract is not.
"""
from __future__ import annotations

import random
from datetime import datetime, timezone

import pandas as pd
from pydantic import ValidationError

from config import CONFIG
from logging_setup import get_logger
from schemas import Order

logger = get_logger(__name__)

ORDER_TYPES = ["dine_in", "takeaway", "delivery"]
PAYMENT_METHODS = ["cash", "card", "wallet"]
ORDER_STATUSES = ["pending", "confirmed", "preparing", "ready", "delivered"]


def load_reference_data():
    df_restaurants = pd.read_csv(CONFIG.restaurants_path)
    df_customers = pd.read_csv(CONFIG.customers_path)
    df_menu_items = pd.read_csv(CONFIG.menu_items_path)

    restaurants = df_restaurants["restaurant_id"].tolist()
    customers = df_customers["customer_id"].tolist()
    menu_by_restaurant = (
        df_menu_items.groupby("restaurant_id").apply(lambda x: x.to_dict("records")).to_dict()
    )
    return restaurants, customers, menu_by_restaurant


def generate_order(restaurants: list, customers: list, menu_by_restaurant: dict) -> dict | None:
    """Builds one schema-validated order payload, or None if validation fails."""
    order_date = datetime.now(timezone.utc)
    restaurant_id = random.choice(restaurants)
    customer_id = random.choice(customers)

    menu_items = menu_by_restaurant[restaurant_id]
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

    payload = {
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

    try:
        Order.model_validate(payload)
    except ValidationError as exc:
        logger.warning("Dropping malformed generated order %s: %s", order_id, exc)
        return None

    return payload
