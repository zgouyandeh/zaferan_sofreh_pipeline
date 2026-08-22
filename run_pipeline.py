"""
run_pipeline.py
-----------------
This script generates synthetic data for the Zaferan Sofreh restaurant analytics pipeline.
It creates reference data (restaurants, menu items, customers), historical orders, and customer reviews,
and writes the output to the specified data directory.
Usage:
    python run_pipeline.py
    python run_pipeline.py --orders 20000 --months-back 12 --review-rate 0.2
"""
from __future__ import annotations

import argparse

import config
from config import CONFIG, PipelineConfig
from logging_setup import get_logger

logger = get_logger(__name__)


def main(cfg: PipelineConfig) -> None:
    # Update global config dynamically so child generators inherit CLI parameters
    config.CONFIG = cfg
    cfg.ensure_dirs()

    logger.info("=== Step 1/3: reference data (restaurants, menu, customers) ===")
    from generators.reference_data import generate_data_for_sql_db
    generate_data_for_sql_db()

    logger.info("=== Step 2/3: historical orders ===")
    from generators.historical_orders import generate_historical_orders
    generate_historical_orders(
        num_orders=cfg.n_historical_orders, 
        months_back=cfg.historical_months_back
    )

    logger.info("=== Step 3/3: customer reviews ===")
    from generators.reviews import generate_customer_reviews
    generate_customer_reviews(review_percentage=cfg.review_rate)

    logger.info("Pipeline complete. Output written to: %s", cfg.data_dir)


def _parse_args() -> PipelineConfig:
    parser = argparse.ArgumentParser(description="Generate synthetic Persian-restaurant data for the bronze layer.")
    parser.add_argument("--customers", type=int, default=CONFIG.n_customers)
    parser.add_argument("--orders", type=int, default=CONFIG.n_historical_orders)
    parser.add_argument("--months-back", type=int, default=CONFIG.historical_months_back)
    parser.add_argument("--review-rate", type=float, default=CONFIG.review_rate)
    parser.add_argument("--seed", type=int, default=CONFIG.random_seed)
    args = parser.parse_args()

    return PipelineConfig(
        random_seed=args.seed,
        n_customers=args.customers,
        n_historical_orders=args.orders,
        historical_months_back=args.months_back,
        review_rate=args.review_rate,
    )


if __name__ == "__main__":
    main(_parse_args())