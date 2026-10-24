"""KPI mart SQL verified on tiny star tables with hand-computed answers."""
from pathlib import Path
from string import Template

import pytest
from pyspark.sql import functions as F

SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "marts"


def run_mart(spark, name, **params):
    sql = Template((SQL_DIR / f"{name}.sql").read_text()).safe_substitute(params)
    return spark.sql(sql)


@pytest.fixture
def star(spark):
    spark.createDataFrame(
        [(20250110, "2025-01", 2025, 2025), (20250210, "2025-02", 2025, 2025), (20250310, "2025-03", 2025, 2025)],
        "date_key int, year_month string, year int, fiscal_year int",
    ).createOrReplaceTempView("dim_date")
    spark.createDataFrame(
        [(1, "D001", "Alpha", "West", "Pune", 100.0), (2, "D002", "Beta", "East", "Kolkata", 100.0)],
        "dealership_key long, dealership_id string, dealership_name string, region string, city string, monthly_sales_target double",
    ).createOrReplaceTempView("dim_dealership")
    spark.createDataFrame(
        [(10, "P1", "M1", "V", "SUV", "Petrol"), (20, "P2", "M2", "V", "Sedan", "Diesel"), (30, "P3", "M3", "V", "MPV", "CNG")],
        "product_key long, product_id string, model string, variant string, segment string, fuel_type string",
    ).createOrReplaceTempView("dim_product")
    return spark


def sales_view(spark, rows):
    spark.createDataFrame(
        rows, "date_key int, dealership_key long, product_key long, quantity int, net_amount double, gross_profit double, discount_pct double"
    ).createOrReplaceTempView("fact_sales")


def test_monthly_growth_ytd_and_rolling(star):
    sales_view(star, [(20250110, 1, 10, 1, 100.0, 10.0, 0.0), (20250210, 1, 10, 1, 150.0, 15.0, 0.0), (20250310, 1, 10, 1, 90.0, 9.0, 0.0)])
    rows = {r["year_month"]: r for r in run_mart(star, "mart_monthly_sales").collect()}
    assert rows["2025-01"]["mom_revenue_growth_pct"] is None
    assert rows["2025-02"]["mom_revenue_growth_pct"] == 50.0
    assert rows["2025-03"]["mom_revenue_growth_pct"] == -40.0
    assert rows["2025-03"]["fiscal_ytd_revenue"] == 340.0
    assert rows["2025-03"]["rolling_3m_avg_revenue"] == pytest.approx(113.33, abs=0.01)
    assert rows["2025-01"]["gross_margin_pct"] == 10.0


def test_dealership_rank_attainment_and_share(star):
    sales_view(star, [(20250110, 1, 10, 1, 120.0, 12.0, 0.0), (20250110, 2, 10, 1, 80.0, 8.0, 0.0)])
    rows = {r["dealership_id"]: r for r in run_mart(star, "mart_dealership_monthly").collect()}
    assert (rows["D001"]["revenue_rank"], rows["D002"]["revenue_rank"]) == (1, 2)
    assert rows["D001"]["target_attainment_pct"] == 120.0 and rows["D002"]["target_attainment_pct"] == 80.0
    assert rows["D001"]["revenue_share_pct"] == 60.0


def test_product_abc_classes(star):
    # revenue shares 60 / 25 / 15 percent. Before-shares are 0 / .60 / .85  => A, A (0.60 < 0.80), B (0.85 < 0.95)
    sales_view(star, [(20250110, 1, 10, 6, 600.0, 60.0, 0.0), (20250110, 1, 20, 2, 250.0, 25.0, 0.0), (20250110, 1, 30, 1, 150.0, 15.0, 0.0)])
    star.createDataFrame(
        [(20250105, 1, 10, 5, 0), (20250105, 1, 20, 4, 0), (20250105, 1, 30, 0, 0)],
        "date_key int, dealership_key long, product_key long, units_on_hand int, units_received int",
    ).withColumn("snapshot_date", F.to_date(F.col("date_key").cast("string"), "yyyyMMdd")).createOrReplaceTempView("fact_inventory_snapshot")
    rows = {r["product_id"]: r for r in run_mart(star, "mart_product_kpis", abc_a=0.80, abc_b=0.95).collect()}
    assert (rows["P1"]["abc_class"], rows["P2"]["abc_class"], rows["P3"]["abc_class"]) == ("A", "A", "B")
    assert rows["P1"]["cumulative_revenue_share_pct"] == 60.0 and rows["P3"]["cumulative_revenue_share_pct"] == 100.0
    assert rows["P1"]["inventory_turnover_for_period"] == pytest.approx(1.2)
    assert rows["P3"]["inventory_turnover_for_period"] is None  # zero average stock => NULL, not a divide-by-zero error


def test_inventory_reconciliation_and_exceptions(star):
    # snapshots are Sundays: 2025-01-05 and 2025-01-12. Week of the second snapshot = Mon 2025-01-06 .. Sun 2025-01-12
    snaps = star.createDataFrame(
        [(20250105, 1, 10, 8, 0, 100.0), (20250112, 1, 10, 6, 0, 100.0), (20250105, 1, 20, 5, 0, 100.0), (20250112, 1, 20, 5, 4, 100.0)],
        "date_key int, dealership_key long, product_key long, units_on_hand int, units_received int, stock_value_at_cost double",
    )
    snaps.withColumn("snapshot_date", F.to_date(F.col("date_key").cast("string"), "yyyyMMdd")).createOrReplaceTempView("fact_inventory_snapshot")
    # P1: sold 2 in the week (Wed 8 Jan) => 8 + 0 - 2 = 6 reconciles. P2: sold 3 but received 4 => 5+4-3 = 6 != 5 -> diff +1
    star.createDataFrame(
        [(20250108, 1, 10, 2), (20250109, 1, 20, 3)], "date_key int, dealership_key long, product_key long, quantity int"
    ).selectExpr("date_key", "dealership_key", "product_key", "quantity", "to_date(cast(date_key as string), 'yyyyMMdd') as sale_date").createOrReplaceTempView("fact_sales")
    rows = {(r["product_id"], r["week_end"].isoformat()): r for r in run_mart(star, "mart_inventory_weekly").collect()}
    assert rows[("P1", "2025-01-05")]["reconciliation_diff"] is None  # first snapshot has no opening stock
    assert rows[("P1", "2025-01-12")]["reconciliation_diff"] == 0
    assert rows[("P2", "2025-01-12")]["reconciliation_diff"] == 1
    assert rows[("P1", "2025-01-12")]["units_sold"] == 2
