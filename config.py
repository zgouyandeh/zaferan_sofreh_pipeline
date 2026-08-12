"""
config.py
---------
Single source of truth for pipeline configuration. All record volumes,
paths and randomness seeds are centralized here and can be overridden via
environment variables, so the same code runs unchanged in local dev,
CI, or a scheduled job (e.g. an Airflow/Databricks Job task).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


@dataclass(frozen=True)
class PipelineConfig:
    # --- Reproducibility -----------------------------------------------
    random_seed: int = _env_int("PIPELINE_RANDOM_SEED", 42)

    # --- Reference / dimension data -------------------------------------
    n_customers: int = _env_int("PIPELINE_N_CUSTOMERS", 500)

    # --- Historical (batch) fact data -----------------------------------
    n_historical_orders: int = _env_int("PIPELINE_N_HISTORICAL_ORDERS", 8000)
    historical_months_back: int = _env_int("PIPELINE_HISTORICAL_MONTHS_BACK", 6)
    review_rate: float = _env_float("PIPELINE_REVIEW_RATE", 0.35)

    # --- Streaming (Event Hub) simulation --------------------------------
    stream_interval_seconds: float = _env_float("PIPELINE_STREAM_INTERVAL_SECONDS", 3)
    stream_batch_size: int = _env_int("PIPELINE_STREAM_BATCH_SIZE", 1)

    # --- Paths ------------------------------------------------------------
    data_dir: Path = field(default_factory=lambda: DATA_DIR)

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

    # Convenience accessors so call sites never hardcode filenames
    @property
    def restaurants_path(self) -> Path:
        return self.data_dir / "restaurants.csv"

    @property
    def menu_items_path(self) -> Path:
        return self.data_dir / "menu_items.csv"

    @property
    def customers_path(self) -> Path:
        return self.data_dir / "customers.csv"

    @property
    def historical_orders_path(self) -> Path:
        return self.data_dir / "historical_orders.csv"

    @property
    def reviews_path(self) -> Path:
        return self.data_dir / "customer_reviews.csv"


CONFIG = PipelineConfig()
