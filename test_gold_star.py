"""Star schema builders on tiny hand-computed inputs."""
import pytest
from pyspark.sql import functions as F

from src.transformations.gold_star import (
    build_dim_date,
    build_fact_inventory,
    build_fact_sales,
    surrogate_key,
)


def test_dim_date_calendar_and_indian_fiscal_year(spark, cfg):
    d = {r["date"].isoformat(): r for r in build_dim_date(spark, cfg).collect()}
    assert d["2025-03-31"]["fiscal_year"] == 2025 and d["2025-03-31"]["fiscal_quarter"] == 4
    assert d["2025-04-01"]["fiscal_year"] == 2026 and d["2025-04-01"]["fiscal_quarter"] == 1
    assert d["2025-12-31"]["fiscal_quarter"] == 3
    assert d["2025-01-04"]["is_weekend"] is True and d["2025-01-06"]["is_weekend"] is False  # Sat / Mon
    assert d["2025-06-15"]["date_key"] == 20250615 and d["2025-06-15"]["year_month"] == "2025-06"
    assert len(d) == 730  # 2025-01-01 .. 2026-12-31


def test_dim_date_is_unique_on_key(spark, cfg):
    df = build_dim_date(spark, cfg)
    assert df.count() == df.select("date_key").distinct().count()


def test_surrogate_key_is_deterministic(spark):
    df = spark.createDataFrame([("D001",), ("D001",), ("D002",)], "dealership_id string")
    keys = [r[0] for r in df.select(surrogate_key("dealership_id")).collect()]
    assert keys[0] == keys[1] != keys[2]


def silver_sales(spark, rows):
    schema = ("sale_id string, sale_date date, sale_month string, dealership_id string, product_id string, quantity int, "
              "unit_price double, discount_pct double, payment_type string, customer_type string, is_late_arrival boolean")
    return spark.createDataFrame(rows, schema.replace("sale_date date", "sale_date string")).withColumn("sale_date", F.to_date("sale_date"))


def test_fact_sales_money_columns(spark):
    sales = silver_sales(spark, [("S1", "2025-03-10", "2025-03", "D001", "P001", 2, 500000.0, 10.0, "Loan", "Retail", False)])
    products = spark.createDataFrame([("P001", 450000.0)], "product_id string, unit_cost double")
    r = build_fact_sales(sales, products).first()
    assert r["gross_amount"] == 1_000_000.0 and r["discount_amount"] == 100_000.0 and r["net_amount"] == 900_000.0
    assert r["cost_amount"] == 900_000.0 and r["gross_profit"] == 0.0
    assert r["date_key"] == 20250310 and r["year_month"] == "2025-03"


def test_fact_sales_negative_margin_is_kept(spark):
    sales = silver_sales(spark, [("S1", "2025-03-10", "2025-03", "D001", "P001", 1, 500000.0, 30.0, "Cash", "Fleet", False)])
    products = spark.createDataFrame([("P001", 450000.0)], "product_id string, unit_cost double")
    assert build_fact_sales(sales, products).first()["gross_profit"] == pytest.approx(-100_000.0)


def test_fact_inventory_stock_value(spark):
    inv = spark.createDataFrame(
        [("2025-03-09", "2025-03", "D001", "P001", 8, 0)],
        "snapshot_date string, snapshot_month string, dealership_id string, product_id string, units_on_hand int, units_received int",
    ).withColumn("snapshot_date", F.to_date("snapshot_date"))
    products = spark.createDataFrame([("P001", 450000.0)], "product_id string, unit_cost double")
    r = build_fact_inventory(inv, products).first()
    assert r["stock_value_at_cost"] == 3_600_000.0 and r["date_key"] == 20250309
