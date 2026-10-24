"""Each data quality rule fires on a minimal bad record; good records pass; optional fields only warn."""
import pytest

from src.validation.entities import apply_rules, build_rules, to_typed


def evaluate(spark, cfg, make_bronze, entity, override, with_refs=True):
    typed = to_typed(make_bronze(entity, [override]), entity)
    if entity in ("sales", "inventory"):
        from pyspark.sql import functions as F

        ok = F.lit(True) if with_refs else F.lit(None).cast("boolean")
        typed = typed.withColumn("_dealer_ok", ok).withColumn("_product_ok", ok)
    row = apply_rules(typed, build_rules(cfg, entity)).select("_dq_errors", "_dq_warnings").first()
    return set(row["_dq_errors"]), set(row["_dq_warnings"])


@pytest.mark.parametrize("entity", ["dealerships", "products", "sales", "inventory"])
def test_good_record_passes(spark, cfg, make_bronze, entity):
    assert evaluate(spark, cfg, make_bronze, entity, {}) == (set(), set())


@pytest.mark.parametrize(
    "entity, override, rule",
    [
        ("sales", {"sale_id": ""}, "missing_sale_id"),
        ("sales", {"sale_id": "X1"}, "invalid_sale_id_format"),
        ("sales", {"sale_date": ""}, "missing_sale_date"),
        ("sales", {"sale_date": "2025-02-31"}, "invalid_sale_date"),
        ("sales", {"sale_date": "10/03/2025"}, "invalid_sale_date"),
        ("sales", {"sale_date": "2019-12-31"}, "sale_date_too_old"),
        ("sales", {"sale_date": "2999-01-01"}, "sale_date_in_future"),
        ("sales", {"quantity": "0"}, "out_of_range_quantity"),
        ("sales", {"quantity": "11"}, "out_of_range_quantity"),
        ("sales", {"quantity": "1.5"}, "invalid_quantity"),
        ("sales", {"quantity": "two"}, "invalid_quantity"),
        ("sales", {"unit_price": "0"}, "out_of_range_unit_price"),
        ("sales", {"unit_price": "abc"}, "invalid_unit_price"),
        ("sales", {"unit_price": "NaN"}, "invalid_unit_price"),
        ("sales", {"discount_pct": "31"}, "out_of_range_discount_pct"),
        ("sales", {"discount_pct": "-1"}, "out_of_range_discount_pct"),
        ("sales", {"payment_type": "Barter"}, "invalid_payment_type"),
        ("sales", {"payment_type": ""}, "missing_payment_type"),
        ("inventory", {"units_on_hand": "-1"}, "out_of_range_units_on_hand"),
        ("inventory", {"units_received": "x"}, "invalid_units_received"),
        ("inventory", {"snapshot_date": ""}, "missing_snapshot_date"),
        ("dealerships", {"dealership_id": "DD1"}, "invalid_dealership_id_format"),
        ("dealerships", {"monthly_sales_target": "0"}, "out_of_range_monthly_sales_target"),
        ("dealerships", {"opened_date": "yesterday"}, "invalid_opened_date"),
        ("products", {"segment": "Spaceship"}, "invalid_segment"),
        ("products", {"fuel_type": "Steam"}, "invalid_fuel_type"),
        ("products", {"unit_cost": "600000"}, "cost_not_below_price"),
        ("products", {"list_price": "-5"}, "out_of_range_list_price"),
    ],
)
def test_error_rules(spark, cfg, make_bronze, entity, override, rule):
    errors, _ = evaluate(spark, cfg, make_bronze, entity, override)
    assert rule in errors


def test_orphan_references_flagged(spark, cfg, make_bronze):
    errors, _ = evaluate(spark, cfg, make_bronze, "sales", {}, with_refs=False)
    assert {"orphan_dealership", "orphan_product"} <= errors


def test_optional_fields_warn_not_reject(spark, cfg, make_bronze):
    errors, warnings = evaluate(spark, cfg, make_bronze, "sales", {"discount_pct": "", "customer_type": ""})
    assert errors == set() and warnings == {"missing_discount_pct", "missing_customer_type"}


def test_garbage_in_optional_field_is_an_error_not_a_warning(spark, cfg, make_bronze):
    errors, _ = evaluate(spark, cfg, make_bronze, "sales", {"discount_pct": "lots"})
    assert "invalid_discount_pct" in errors


def test_ids_are_normalised(spark, make_bronze):
    row = to_typed(make_bronze("sales", [{"sale_id": " s0000001 ", "dealership_id": "d001"}]), "sales").first()
    assert (row["sale_id"], row["dealership_id"]) == ("S0000001", "D001")

