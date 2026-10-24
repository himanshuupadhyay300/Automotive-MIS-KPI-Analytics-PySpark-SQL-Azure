"""End-to-end: generate data, load batch 1, then batch 2 (late data, corrections, duplicates), then re-run.
Compare against the expectations the generator computed independently of the pipeline."""
import json
import shutil
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.ingestion.generate_synthetic_data import generate
from src.pipeline import run_pipeline
from src.utils import storage
from src.utils.config import load_config, table_path
from tests.conftest import CONFIG_PATH


@pytest.fixture(scope="module")
def world(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("e2e")
    landing = root / "landing"
    expected = generate(landing, seed=42)
    held = root / "held"
    held.mkdir()
    for name in ("dealerships_002.csv", "sales_batch_002.csv", "inventory_batch_002.csv"):
        shutil.move(str(landing / name), held / name)
    cfg = load_config(CONFIG_PATH, {
        "storage": {"base_path": str(root / "lake"), "input_path": str(landing), "reports_path": str(root / "reports")},
        "marts": {"sql_dir": str(Path(CONFIG_PATH).parents[1] / "sql" / "marts")},
        "spark": {"shuffle_partitions": 2},
    })

    def table(layer, name):
        return storage.read_table(spark, table_path(cfg, layer, name), cfg["storage"]["format"])

    run1 = run_pipeline(cfg, spark=spark)
    recon_after_1 = table("gold", "mart_inventory_weekly").filter("reconciliation_diff <> 0").count()
    reports1 = sorted(Path(cfg["storage"]["reports_path"]).glob("dq_report_*.json"))
    for f in held.iterdir():
        shutil.move(str(f), landing / f.name)
    run2 = run_pipeline(cfg, spark=spark)
    reports = sorted(Path(cfg["storage"]["reports_path"]).glob("dq_report_*.json"))
    run3 = run_pipeline(cfg, spark=spark)
    return dict(cfg=cfg, expected=expected, run1=run1, run2=run2, run3=run3, table=table, recon1=recon_after_1,
                dq1=json.loads(reports1[0].read_text()), dq2=json.loads(reports[-1].read_text()))


def test_batch1_row_counts(world):
    e, dq = world["expected"]["after_batch_1"], world["dq1"]["entities"]
    for entity in ("sales", "inventory"):
        assert dq[entity]["bronze_rows"] == e[entity]["bronze_rows"]
        assert dq[entity]["rejected_rows"] == e[entity]["rejected_rows"]
        assert dq[entity]["silver_rows_after"] == e[entity]["silver_rows"]
        assert dq[entity]["in_batch_duplicate_rows_removed"] == e[entity]["in_batch_duplicate_rows_removed"]
    assert dq["sales"]["rows_with_warnings"] == e["sales"]["rows_with_warnings"]
    assert dq["sales"]["late_arrival_rows"] == 0
    assert dq["dealerships"]["silver_rows_after"] == 6 and dq["products"]["silver_rows_after"] == 12


def test_batch1_rejections_by_rule(world):
    assert world["dq1"]["entities"]["sales"]["rejected_by_rule"] == {
        "missing_sale_date": 3, "out_of_range_quantity": 2, "orphan_dealership": 2,
        "out_of_range_discount_pct": 2, "invalid_unit_price": 1, "invalid_sale_date": 1, "missing_sale_id": 1,
    }


def test_reconciliation_flags_exactly_the_cells_with_missing_late_sales(world):
    assert world["recon1"] == world["expected"]["after_batch_1"]["reconciliation_exceptions"]
    assert world["table"]("gold", "mart_inventory_weekly").filter("reconciliation_diff <> 0").count() == 0  # cleared after batch 2


def test_batch2_late_corrections_and_duplicates(world):
    e, dq = world["expected"]["after_batch_2"], world["dq2"]["entities"]
    assert dq["sales"]["late_arrival_rows"] == e["sales"]["late_arrival_rows"] == 12
    assert dq["sales"]["rows_matching_existing_silver"] == e["sales"]["rows_matching_existing_silver"]
    assert dq["sales"]["rows_changing_existing_silver"] == e["sales"]["rows_changing_existing_silver"]
    assert dq["sales"]["rejected_rows"] == e["sales"]["rejected_rows"]
    assert dq["dealerships"]["rows_changing_existing_silver"] == 1
    assert dq["sales"]["silver_rows_after"] == e["sales"]["silver_rows"]
    assert dq["inventory"]["silver_rows_after"] == e["inventory"]["silver_rows"]


def test_star_schema_row_counts_and_marts(world):
    g, t = world["expected"]["after_batch_2"]["gold"], world["table"]
    assert t("gold", "fact_sales").count() == g["fact_sales_rows"]
    assert t("gold", "fact_inventory_snapshot").count() == g["fact_inventory_rows"]
    for mart in ("mart_monthly_sales", "mart_dealership_monthly", "mart_product_kpis", "mart_inventory_weekly"):
        assert t("gold", mart).count() == g[f"{mart}_rows"], mart


def test_financial_totals_match_independent_calculation(world):
    g = world["expected"]["after_batch_2"]["gold"]
    tot = world["table"]("gold", "fact_sales").agg(
        F.sum("net_amount").alias("rev"), F.sum("gross_profit").alias("gp"), F.sum("quantity").alias("u")).first()
    assert tot["rev"] == pytest.approx(g["total_net_revenue"], abs=1.0)
    assert tot["gp"] == pytest.approx(g["total_gross_profit"], abs=1.0)
    assert tot["u"] == g["total_units"]
    mart_rev = world["table"]("gold", "mart_monthly_sales").agg(F.sum("net_revenue")).first()[0]
    assert mart_rev == pytest.approx(g["total_net_revenue"], abs=1.0)


def test_correction_applied_and_scd1_dimension_updated(world):
    sale_id = world["expected"]["corrected_sale_ids"][0]
    silver = world["table"]("silver", "sales").filter(F.col("sale_id") == sale_id).first()
    assert silver["discount_pct"] > 0
    d2 = world["table"]("gold", "dim_dealership").filter("dealership_id = 'D002'").first()
    assert d2["monthly_sales_target"] == 49_500_000.0


def test_late_sales_are_flagged_in_fact(world):
    late = world["table"]("gold", "fact_sales").filter("is_late_arrival").count()
    assert late == 12


def test_no_duplicate_business_keys_and_valid_star_keys(world):
    t = world["table"]
    fs = t("gold", "fact_sales")
    assert fs.count() == fs.select("sale_id").distinct().count()
    assert fs.join(t("gold", "dim_product"), "product_key", "left_anti").count() == 0
    assert fs.join(t("gold", "dim_dealership"), "dealership_key", "left_anti").count() == 0
    assert fs.join(t("gold", "dim_date"), "date_key", "left_anti").count() == 0


def test_abc_classification_sane(world):
    rows = world["table"]("gold", "mart_product_kpis").collect()
    assert {r["abc_class"] for r in rows} <= {"A", "B", "C"} and any(r["abc_class"] == "A" for r in rows)
    assert max(r["cumulative_revenue_share_pct"] for r in rows) == pytest.approx(100.0, abs=0.05)


def test_rerun_is_a_noop(world):
    r3 = world["run3"]
    assert r3["stages"]["bronze"]["files_ingested"] == 0 and r3["stages"]["silver"]["batches_processed"] == 0
    assert r3["table_row_counts"] == world["run2"]["table_row_counts"]


def test_reports_written(world):
    assert list(Path(world["cfg"]["storage"]["reports_path"]).glob("pipeline_summary_*.md"))


def test_dimension_surrogate_keys_are_unique(world):
    for dim, key, bkey in (("dim_dealership", "dealership_key", "dealership_id"), ("dim_product", "product_key", "product_id"), ("dim_date", "date_key", "date")):
        df = world["table"]("gold", dim)
        assert df.select(key).distinct().count() == df.select(bkey).distinct().count() == df.count(), dim
