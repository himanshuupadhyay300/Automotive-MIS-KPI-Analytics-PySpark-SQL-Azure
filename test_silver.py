"""Silver behaviour: dedupe, corrections, imputation, SCD1 reference upsert, referential integrity, idempotency."""
from pyspark.sql import functions as F

from src.transformations.silver import process_entity
from src.utils import storage
from src.utils.config import table_path


def silver(spark, cfg, entity):
    return storage.read_table(spark, table_path(cfg, "silver", entity), "parquet")


def load_refs(spark, cfg, make_bronze):
    process_entity(spark, cfg, "dealerships", make_bronze("dealerships", [{}], "r1"), "r1")
    process_entity(spark, cfg, "products", make_bronze("products", [{}], "r1"), "r1")


def test_exact_duplicates_collapse_and_conflicts_counted(spark, cfg, make_bronze):
    load_refs(spark, cfg, make_bronze)
    rows = [{}, {}, {"sale_id": "S0000002"}, {"sale_id": "S0000002", "discount_pct": "9"}]
    m = process_entity(spark, cfg, "sales", make_bronze("sales", rows), "b1")
    assert m["valid_rows"] == 4 and m["in_batch_duplicate_rows_removed"] == 2
    assert m["conflicting_duplicate_keys"] == 1
    assert silver(spark, cfg, "sales").count() == 2
    assert silver(spark, cfg, "sales").filter("sale_id='S0000002'").first()["discount_pct"] == 9.0  # latest row wins


def test_later_batch_corrects_existing_sale(spark, cfg, make_bronze):
    load_refs(spark, cfg, make_bronze)
    process_entity(spark, cfg, "sales", make_bronze("sales", [{"discount_pct": "5"}], "b1", "2026-01-10 00:00:00"), "b1")
    m = process_entity(spark, cfg, "sales", make_bronze("sales", [{"discount_pct": "7"}], "b2", "2026-01-11 00:00:00"), "b2")
    assert (m["rows_matching_existing_silver"], m["rows_changing_existing_silver"]) == (1, 1)
    s = silver(spark, cfg, "sales")
    assert s.count() == 1 and s.first()["discount_pct"] == 7.0


def test_identical_resend_matches_but_does_not_change(spark, cfg, make_bronze):
    load_refs(spark, cfg, make_bronze)
    process_entity(spark, cfg, "sales", make_bronze("sales", [{}], "b1", "2026-01-10 00:00:00"), "b1")
    m = process_entity(spark, cfg, "sales", make_bronze("sales", [{}], "b2", "2026-01-11 00:00:00"), "b2")
    assert (m["rows_matching_existing_silver"], m["rows_changing_existing_silver"]) == (1, 0)


def test_missing_optional_values_are_imputed_with_warning(spark, cfg, make_bronze):
    load_refs(spark, cfg, make_bronze)
    m = process_entity(spark, cfg, "sales", make_bronze("sales", [{"discount_pct": "", "customer_type": ""}]), "b1")
    row = silver(spark, cfg, "sales").first()
    assert (row["discount_pct"], row["customer_type"]) == (0.0, "Unknown")
    assert m["rows_with_warnings"] == 1 and set(row["_dq_warnings"]) == {"missing_discount_pct", "missing_customer_type"}


def test_orphans_and_invalid_rows_are_quarantined(spark, cfg, make_bronze):
    load_refs(spark, cfg, make_bronze)
    rows = [{}, {"sale_id": "S0000002", "dealership_id": "D999"}, {"sale_id": "S0000003", "product_id": "P999"},
            {"sale_id": "S0000004", "quantity": "-1"}]
    m = process_entity(spark, cfg, "sales", make_bronze("sales", rows), "b1")
    assert (m["valid_rows"], m["rejected_rows"]) == (1, 3)
    rejects = storage.read_table(spark, table_path(cfg, "rejects", "sales"), "parquet")
    reasons = {r[0] for r in rejects.select(F.explode("_dq_errors")).distinct().collect()}
    assert {"orphan_dealership", "orphan_product", "out_of_range_quantity"} <= reasons
    assert silver(spark, cfg, "sales").count() == 1


def test_sales_before_reference_data_are_all_orphans(spark, cfg, make_bronze):
    m = process_entity(spark, cfg, "sales", make_bronze("sales", [{}]), "b1")
    assert m["valid_rows"] == 0 and m["rejected_by_rule"]["orphan_dealership"] == 1


def test_late_arrival_flagged_per_dealership(spark, cfg, make_bronze):
    load_refs(spark, cfg, make_bronze)
    process_entity(spark, cfg, "sales", make_bronze("sales", [{"sale_date": "2025-03-20"}], "b1", "2026-01-10 00:00:00"), "b1")
    m = process_entity(spark, cfg, "sales", make_bronze("sales", [{"sale_id": "S0000002", "sale_date": "2025-03-10"}], "b2", "2026-01-11 00:00:00"), "b2")
    assert m["late_arrival_rows"] == 1
    assert silver(spark, cfg, "sales").filter("sale_id='S0000002'").first()["is_late_arrival"] is True


def test_reference_upsert_is_scd_type_1(spark, cfg, make_bronze):
    process_entity(spark, cfg, "dealerships", make_bronze("dealerships", [{}, {"dealership_id": "D002"}], "r1", "2026-01-10 00:00:00"), "r1")
    m = process_entity(spark, cfg, "dealerships", make_bronze("dealerships", [{"monthly_sales_target": "2000000"}], "r2", "2026-01-11 00:00:00"), "r2")
    assert m["rows_changing_existing_silver"] == 1
    rows = {r["dealership_id"]: r["monthly_sales_target"] for r in silver(spark, cfg, "dealerships").collect()}
    assert rows == {"D001": 2000000.0, "D002": 1000000.0}  # D001 overwritten, D002 untouched, no duplicates


def test_reprocessing_same_batch_is_idempotent(spark, cfg, make_bronze):
    load_refs(spark, cfg, make_bronze)
    bronze = make_bronze("sales", [{}, {"sale_id": "S0000002", "sale_date": "2025-04-02"}, {"sale_id": "S0000003", "quantity": "0"}])
    process_entity(spark, cfg, "sales", bronze, "b1")
    first = sorted(tuple(sorted(r.asDict().items())) for r in silver(spark, cfg, "sales").drop("_processed_at").collect())
    process_entity(spark, cfg, "sales", bronze, "b1")
    second = sorted(tuple(sorted(r.asDict().items())) for r in silver(spark, cfg, "sales").drop("_processed_at").collect())
    assert first == second
    assert storage.read_table(spark, table_path(cfg, "rejects", "sales"), "parquet").count() == 1


def test_inventory_snapshot_key_is_date_dealer_product(spark, cfg, make_bronze):
    load_refs(spark, cfg, make_bronze)
    rows = [{}, {"units_on_hand": "9"}, {"snapshot_date": "2025-03-16"}]
    m = process_entity(spark, cfg, "inventory", make_bronze("inventory", rows), "b1")
    assert m["in_batch_duplicate_rows_removed"] == 1 and silver(spark, cfg, "inventory").count() == 2
