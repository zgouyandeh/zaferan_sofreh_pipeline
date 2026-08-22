"""
generators/historical_orders.py
---------------------------------
This module generates synthetic historical order data for the Zaferan Sofreh restaurant analytics pipeline.
It creates a specified number of orders over a given time range, ensuring that each order adheres to the defined 
Pydantic schema for data integrity. The generated orders are written to a CSV file for use in the bronze layer of 
the pipeline.
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
ORDER_STATUSES = ["delivered", "completed"]


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


def _get_random_valid_timestamp(start_date: datetime, end_date: datetime) -> datetime:
    """Generates timestamps with Persian restaurant seasonality and strict opening hours."""
    while True:
        days_offset = random.randint(0, (end_date - start_date).days)
        candidate_date = start_date + timedelta(days=days_offset)
        weekday = candidate_date.weekday()  # Monday=0, Thursday=3, Friday=4, Sunday=6

        # Assign probability weight based on weekly peak days
        if weekday == 4:  # Friday (Peak day)
            day_weight = 0.95
        elif weekday == 3:  # Thursday (High peak for dinner)
            day_weight = 0.75
        else:  # Regular days
            day_weight = 0.40

        if random.random() > day_weight:
            continue

        # Select meal slot based on operating hours
        slot = random.choices(
            population=["brunch", "lunch", "dinner"],
            weights=[0.15, 0.45, 0.40] if weekday == 4 else [0.10, 0.40, 0.50]
        )[0]

        if slot == "brunch":
            hour = 10
            minute = random.randint(0, 59)
        elif slot == "lunch":
            hour = random.randint(12, 15)
            minute = random.randint(0, 59) if hour < 15 else random.randint(0, 30)  # Ends at 15:30 (4:30 PM = 16:30)
        else:  # dinner
            hour = random.randint(19, 21)
            minute = random.randint(0, 59)

        # Enforce Thursday evening bias for dinner
        if weekday == 3 and slot != "dinner" and random.random() < 0.4:
            continue

        return candidate_date.replace(hour=hour, minute=minute, second=random.randint(0, 59))


def _filter_menu_by_slot(menu_items: list, order_time: datetime) -> list:
    hour = order_time.hour
    if hour == 10:
        slot_items = [i for i in menu_items if i["category"] in ["Brunch", "Beverage", "Bread"]]
    elif 12 <= hour < 16:
        slot_items = [i for i in menu_items if i["category"] in ["Starter", "Main Course", "Beverage", "Bread", "Dessert"]]
    else:  # Dinner (19 to 22)
        slot_items = [i for i in menu_items if i["category"] in ["Starter", "Main Course", "Beverage", "Bread", "Dessert"]]
    
    return slot_items if slot_items else menu_items


def _build_order_payload(order_date: datetime, restaurant_id: str, customer_id: str, menu_items: list) -> dict:
    available_items = _filter_menu_by_slot(menu_items, order_date)
    num_items = random.randint(1, min(5, len(available_items)))
    selected_items = random.sample(available_items, num_items)

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
        order_date = _get_random_valid_timestamp(start_date, end_date)
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
                "items": json.dumps(payload["items"]),
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