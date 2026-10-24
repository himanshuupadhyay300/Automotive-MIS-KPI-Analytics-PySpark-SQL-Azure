"""Gold layer, part 1: star schema (dimensions + facts) built from Silver.

Dimensions (small, fully rebuilt each run):   dim_date, dim_dealership, dim_product
Facts (partitioned by year_month, rebuilt ONLY for months touched by new data):
    fact_sales               grain = one sale transaction
    fact_inventory_snapshot  grain = one dealership x product x weekly snapshot

Surrogate keys are deterministic 64-bit hashes of the business key (xxhash64). Being deterministic they are
stable across re-runs and need no lookup table, which keeps the load idempotent and parallel.
"""
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils import storage
from src.utils.config import table_path
from src.utils.logging_utils import get_logger

log = get_logger(__name__)


def surrogate_key(business_key: str) -> Any:
    """Deterministic surrogate key for a business-key column."""
    return F.xxhash64(F.col(business_key))


def build_dim_date(spark: SparkSession, cfg: dict[str, Any]) -> DataFrame:
    """Calendar dimension with an Indian-style fiscal year (default April-March, labelled by its ending year)."""
    g = cfg["gold"]
    fy_month = int(g["fiscal_year_start_month"])
    days = spark.range(1).select(
        F.explode(F.sequence(F.to_date(F.lit(g["dim_date_start"])), F.to_date(F.lit(g["dim_date_end"])))).alias("date")
    )
    month = F.month("date")
    fiscal_year = F.year("date") + (F.when(month >= fy_month, 1).otherwise(0) if fy_month != 1 else F.lit(0))
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        month.alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofweek("date").alias("day_of_week"),  # 1 = Sunday
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        fiscal_year.alias("fiscal_year"),
        (F.floor(((month - fy_month + 12) % 12) / 3) + 1).alias("fiscal_quarter"),
    )


def build_dim_dealership(silver: DataFrame) -> DataFrame:
    return silver.select(
        surrogate_key("dealership_id").alias("dealership_key"),
        "dealership_id", "dealership_name", "city", "state", "region", "opened_date", "monthly_sales_target",
    )


def build_dim_product(silver: DataFrame) -> DataFrame:
    return silver.select(
        surrogate_key("product_id").alias("product_key"),
        "product_id", "model", "variant",
        F.concat_ws(" ", "model", "variant").alias("product_label"),
        "segment", "fuel_type", "list_price", "unit_cost",
    )


def build_fact_sales(silver_sales: DataFrame, silver_products: DataFrame) -> DataFrame:
    """One row per sale with money columns derived from quantity, price, discount and product cost."""
    cost = silver_products.select("product_id", "unit_cost")
    f = silver_sales.join(F.broadcast(cost), "product_id", "left")
    gross = F.col("quantity") * F.col("unit_price")
    discount = gross * F.col("discount_pct") / 100.0
    net = gross - discount
    cost_amt = F.col("quantity") * F.col("unit_cost")
    return f.select(
        "sale_id",
        F.date_format("sale_date", "yyyyMMdd").cast("int").alias("date_key"),
        surrogate_key("dealership_id").alias("dealership_key"),
        surrogate_key("product_id").alias("product_key"),
        "sale_date", "quantity", "unit_price", "discount_pct",
        F.round(gross, 2).alias("gross_amount"),
        F.round(discount, 2).alias("discount_amount"),
        F.round(net, 2).alias("net_amount"),
        F.round(cost_amt, 2).alias("cost_amount"),
        F.round(net - cost_amt, 2).alias("gross_profit"),
        "payment_type", "customer_type", "is_late_arrival",
        F.col("sale_month").alias("year_month"),
    )


def build_fact_inventory(silver_inv: DataFrame, silver_products: DataFrame) -> DataFrame:
    cost = silver_products.select("product_id", "unit_cost")
    f = silver_inv.join(F.broadcast(cost), "product_id", "left")
    return f.select(
        F.date_format("snapshot_date", "yyyyMMdd").cast("int").alias("date_key"),
        surrogate_key("dealership_id").alias("dealership_key"),
        surrogate_key("product_id").alias("product_key"),
        "snapshot_date", "units_on_hand", "units_received",
        F.round(F.col("units_on_hand") * F.col("unit_cost"), 2).alias("stock_value_at_cost"),
        F.col("snapshot_month").alias("year_month"),
    )


def _months(df: DataFrame, col: str) -> list[str]:
    return sorted({str(r[0]) for r in df.select(col).distinct().collect()})


def run_gold_star(
    spark: SparkSession,
    cfg: dict[str, Any],
    sales_months: list[str],
    inventory_months: list[str],
    rebuild_all: bool = False,
) -> dict[str, Any]:
    """Rebuild dimensions and the fact partitions affected by new data (all of them if ``rebuild_all``)."""
    fmt = cfg["storage"]["format"]
    silver = {e: table_path(cfg, "silver", e) for e in ("dealerships", "products", "sales", "inventory")}
    if not all(storage.table_exists(spark, silver[e], fmt) for e in ("dealerships", "products")):
        log.info("gold star: reference data not available yet")
        return {}

    dealers = storage.read_table(spark, silver["dealerships"], fmt)
    products = storage.read_table(spark, silver["products"], fmt)
    storage.overwrite_table(build_dim_date(spark, cfg), table_path(cfg, "gold", "dim_date"), fmt)
    storage.overwrite_table(build_dim_dealership(dealers), table_path(cfg, "gold", "dim_dealership"), fmt)
    storage.overwrite_table(build_dim_product(products), table_path(cfg, "gold", "dim_product"), fmt)
    result: dict[str, Any] = {"dimensions": 3}

    if storage.table_exists(spark, silver["sales"], fmt):
        sales = storage.read_table(spark, silver["sales"], fmt)
        months = _months(sales, "sale_month") if rebuild_all else sales_months
        if months:
            part = build_fact_sales(sales.filter(F.col("sale_month").isin(months)), products)
            storage.replace_partitions(spark, part, table_path(cfg, "gold", "fact_sales"), fmt, "year_month", months)
        result["fact_sales_months"] = len(months)
    if storage.table_exists(spark, silver["inventory"], fmt):
        inv = storage.read_table(spark, silver["inventory"], fmt)
        months = _months(inv, "snapshot_month") if rebuild_all else inventory_months
        if months:
            part = build_fact_inventory(inv.filter(F.col("snapshot_month").isin(months)), products)
            storage.replace_partitions(spark, part, table_path(cfg, "gold", "fact_inventory_snapshot"), fmt, "year_month", months)
        result["fact_inventory_months"] = len(months)
    log.info("gold star complete", extra={"ctx": result})
    return result
