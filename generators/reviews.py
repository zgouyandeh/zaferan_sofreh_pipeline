"""
generators/reviews.py
-----------------------
This module generates synthetic customer reviews for the Zaferan Sofreh restaurant analytics pipeline.
It creates reviews for a subset of historical orders, ensuring that each review adheres to the defined
Pydantic schema for data integrity. The generated reviews are written to a CSV file for use in the bronze 
layer of the pipeline.
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta

import pandas as pd
from pydantic import ValidationError

from config import CONFIG
from logging_setup import get_logger
from schemas import Review

logger = get_logger(__name__)

random.seed(CONFIG.random_seed)

REVIEW_TEMPLATES = {
    5: [
        "Absolutely amazing {dishes}! The {highlight} was cooked to perfection. Fresh ingredients and authentic Persian flavors. Highly recommend!",
        "Outstanding experience! The {dishes} exceeded all expectations. {highlight} was the star of the sofreh. Will definitely order again!",
        "Best Persian food I've had! {dishes} were incredible. {highlight} had the perfect balance of saffron and spice. Five stars!",
        "Exceptional quality! Ordered {dishes} and everything was delicious. The {highlight} melted in my mouth. Perfect!",
        "Hands down the best {highlight} I've ever had! {dishes} were all prepared beautifully. Fresh and flavorful!",
        "Incredible meal! {dishes} arrived hot and fresh. The {highlight} was absolutely divine. Highly satisfied!",
    ],
    4: [
        "Really good {dishes}! The {highlight} was delicious. Slight delay in delivery but food quality made up for it.",
        "Great food overall. {dishes} were tasty, especially the {highlight}. Would order again!",
        "Enjoyed the {dishes}! {highlight} was very good. Portion sizes were generous. Recommend!",
        "Very satisfied! {dishes} were fresh and flavorful. {highlight} was the standout dish.",
        "Good quality food. {dishes} were nicely done. {highlight} could use a touch more saffron but still good!",
    ],
    3: [
        "Decent food but nothing special. {dishes} were okay. {highlight} lacked the punch I expected.",
        "Average experience. {dishes} were fine but {highlight} was a bit bland. Room for improvement.",
        "Mixed feelings. {dishes} were acceptable. {highlight} was decent but portion was small for the price.",
        "It was okay. {dishes} arrived lukewarm. {highlight} tasted fine but could be better.",
        "Mediocre. {dishes} were fine but forgettable. {highlight} didn't stand out.",
    ],
    2: [
        "Disappointed with {dishes}. {highlight} was cold when it arrived. Not worth the money.",
        "Below expectations. {dishes} were underwhelming. {highlight} was overcooked and dry.",
        "Not good. {dishes} arrived late and cold. {highlight} had barely any flavor. Poor quality.",
        "Unsatisfactory. {dishes} were not fresh. {highlight} tasted reheated. Won't order again.",
    ],
    1: [
        "Terrible experience! {dishes} were all inedible. {highlight} was completely burnt. Waste of money!",
        "Absolutely horrible! {dishes} arrived ice cold after a 2 hour delay. {highlight} was spoiled. Disgusting!",
        "Worst meal ever! {dishes} were all wrong. {highlight} made me sick. Never ordering again!",
        "Disaster! {dishes} were all stale. {highlight} had a weird smell. Completely unacceptable!",
    ],
}

RATING_WEIGHTS = {5: 0.50, 4: 0.25, 3: 0.12, 2: 0.08, 1: 0.05}


def _extract_items_from_order(items_json: str) -> list[str]:
    items = json.loads(items_json)
    return [item["name"] for item in items]


def _format_dishes(dishes_list: list[str]) -> str:
    if len(dishes_list) == 1:
        return dishes_list[0]
    if len(dishes_list) == 2:
        return f"{dishes_list[0]} and {dishes_list[1]}"
    return f"{', '.join(dishes_list[:-1])}, and {dishes_list[-1]}"


def _generate_review_text(rating: int, dishes_list: list[str]) -> str:
    template = random.choice(REVIEW_TEMPLATES[rating])
    dishes_formatted = _format_dishes(dishes_list)
    highlight = random.choice(dishes_list)
    review = template.format(dishes=dishes_formatted, highlight=highlight)
    return review.replace(",", " ")


def generate_customer_reviews(review_percentage: float | None = None) -> pd.DataFrame:
    review_percentage = review_percentage if review_percentage is not None else CONFIG.review_rate

    df_orders = pd.read_csv(CONFIG.historical_orders_path)

    ratings_pool = []
    for rating, weight in RATING_WEIGHTS.items():
        ratings_pool.extend([rating] * int(weight * 100))

    logger.info("Generating reviews from %d orders (target rate=%.0f%%)", len(df_orders), review_percentage * 100)

    rows = []
    rejected = 0

    for _, order in df_orders.iterrows():
        if random.random() > review_percentage:
            continue

        dishes = _extract_items_from_order(order["items"])
        rating = random.choice(ratings_pool)
        review_text = _generate_review_text(rating, dishes)

        order_date = datetime.fromisoformat(order["timestamp"])
        review_ts = order_date + timedelta(days=random.randint(1, 7))
        review_id = f"REV-{len(rows) + 1:06d}"

        payload = {
            "review_id": review_id,
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "restaurant_id": order["restaurant_id"],
            "review_text": review_text,
            "rating": rating,
            "review_timestamp": review_ts.isoformat(),
        }

        try:
            validated = Review.model_validate(payload)
        except ValidationError as exc:
            rejected += 1
            logger.warning("Rejected malformed review %s: %s", payload.get("review_id"), exc)
            continue

        rows.append(validated.model_dump(mode="json"))

    df_reviews = pd.DataFrame(rows).sort_values("review_timestamp").reset_index(drop=True)

    CONFIG.ensure_dirs()
    df_reviews.to_csv(CONFIG.reviews_path, index=False)

    logger.info("Wrote customer_reviews.csv -> %d rows (%d rejected by schema)", len(df_reviews), rejected)
    logger.info("Rating distribution:\n%s", df_reviews["rating"].value_counts().sort_index().to_string())
    return df_reviews


if __name__ == "__main__":
    generate_customer_reviews()
