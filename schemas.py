"""
schemas.py
----------
Data contracts for every record type produced by this pipeline.

Both the batch generator (01_historical_orders) and the streaming
producer (04_eventhub_orders) build the *same* Order/OrderItem shape, so
a single Pydantic schema is validated on both paths. This mirrors the
"schema-on-write" contract you'd enforce with Auto Loader's schema
evolution / a Great Expectations suite in the real bronze layer, and
catches malformed records before they ever reach storage.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List

from pydantic import BaseModel, Field, field_validator


class OrderType(str, Enum):
    DINE_IN = "dine_in"
    TAKEAWAY = "takeaway"
    DELIVERY = "delivery"


class PaymentMethod(str, Enum):
    CASH = "cash"
    CARD = "card"
    WALLET = "wallet"


class OrderStatus(str, Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    PREPARING = "preparing"
    READY = "ready"
    DELIVERED = "delivered"
    COMPLETED = "completed"


class OrderItem(BaseModel):
    item_id: str
    name: str
    category: str
    quantity: int = Field(gt=0, le=20)
    unit_price: float = Field(gt=0)
    subtotal: float = Field(gt=0)

    @field_validator("subtotal")
    @classmethod
    def subtotal_matches_price(cls, v: float, info) -> float:
        unit_price = info.data.get("unit_price")
        quantity = info.data.get("quantity")
        if unit_price is not None and quantity is not None:
            expected = round(unit_price * quantity, 2)
            if abs(v - expected) > 0.01:
                raise ValueError(
                    f"subtotal {v} does not match unit_price*quantity={expected}"
                )
        return v


class Order(BaseModel):
    order_id: str
    timestamp: datetime
    restaurant_id: str
    customer_id: str
    order_type: OrderType
    items: List[OrderItem] = Field(min_length=1)
    total_amount: float = Field(gt=0)
    payment_method: PaymentMethod
    order_status: OrderStatus
    created_at: datetime

    @field_validator("total_amount")
    @classmethod
    def total_matches_items(cls, v: float, info) -> float:
        items = info.data.get("items")
        if items:
            expected = round(sum(i.subtotal for i in items), 2)
            if abs(v - expected) > 0.05:
                raise ValueError(
                    f"total_amount {v} does not match sum(items.subtotal)={expected}"
                )
        return v


class Review(BaseModel):
    review_id: str
    order_id: str
    customer_id: str
    restaurant_id: str
    review_text: str
    rating: int = Field(ge=1, le=5)
    review_timestamp: datetime
