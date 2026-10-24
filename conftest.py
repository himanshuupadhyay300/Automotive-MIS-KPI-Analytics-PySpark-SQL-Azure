"""Shared fixtures: one local SparkSession per session, isolated temp lakehouses, Bronze-shaped DataFrames."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from src.utils.config import load_config
from src.utils.spark_session import get_spark
from src.validation.entities import RAW_COLUMNS

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "pipeline_config.yaml"

GOOD = {
    "dealerships": {"dealership_id": "D001", "dealership_name": "Test Motors", "city": "Pune", "state": "MH",
                    "region": "West", "opened_date": "2012-03-15", "monthly_sales_target": "1000000"},
    "products": {"product_id": "P001", "model": "Zephyr", "variant": "LX", "segment": "Hatchback",
                 "fuel_type": "Petrol", "list_price": "500000", "unit_cost": "450000"},
    "sales": {"sale_id": "S0000001", "sale_date": "2025-03-10", "dealership_id": "D001", "product_id": "P001",
              "quantity": "1", "unit_price": "500000", "discount_pct": "5", "payment_type": "Loan", "customer_type": "Retail"},
    "inventory": {"snapshot_date": "2025-03-09", "dealership_id": "D001", "product_id": "P001",
                  "units_on_hand": "8", "units_received": "0"},
}


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = get_spark(load_config(CONFIG_PATH, {"spark": {"shuffle_partitions": 2}}))
    yield session
    session.stop()


@pytest.fixture
def cfg(tmp_path: Path) -> dict:
    return load_config(
        CONFIG_PATH,
        {
            "storage": {"base_path": str(tmp_path / "lake"), "input_path": str(tmp_path / "landing"),
                        "reports_path": str(tmp_path / "reports")},
            "marts": {"sql_dir": str(Path(__file__).resolve().parents[1] / "sql" / "marts")},
            "spark": {"shuffle_partitions": 2},
        },
    )


@pytest.fixture
def make_bronze(spark: SparkSession):
    """Factory: (entity, [dict overrides]) -> Bronze-shaped DataFrame (all text + lineage columns)."""

    def _make(entity: str, rows: list[dict], batch_id: str = "b1", ingested_at: str = "2026-01-10 00:00:00") -> DataFrame:
        data = []
        for i, override in enumerate(rows):
            row = {**GOOD[entity], **override}
            data.append(tuple(row.get(c) for c in RAW_COLUMNS[entity])
                        + (batch_id, datetime.fromisoformat(ingested_at), "file_a.csv", i))
        schema = ", ".join(f"{c} string" for c in RAW_COLUMNS[entity]) + ", _batch_id string, _ingested_at timestamp, _source_file string, _row_id long"
        return spark.createDataFrame(data, schema=schema)

    return _make
