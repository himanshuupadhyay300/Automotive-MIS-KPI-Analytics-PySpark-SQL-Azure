"""Entity catalogue (schemas, keys, typing) and declarative data quality rules.

Four entities flow through the pipeline:
  dealerships, products  -> reference data (small, upserted in full, SCD Type 1)
  sales, inventory       -> transactional data (incremental, partitioned by month)

A Rule is (name, severity, failed-expression). ``error`` => the row is quarantined;
``warning`` => the row is kept (missing optional values are imputed and flagged).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# --------------------------------------------------------------------------------------------------
# Entity catalogue
# --------------------------------------------------------------------------------------------------
# field kinds: id (upper-cased text) | text | date | int | double
FIELDS: dict[str, list[tuple[str, str]]] = {
    "dealerships": [
        ("dealership_id", "id"), ("dealership_name", "text"), ("city", "text"), ("state", "text"),
        ("region", "text"), ("opened_date", "date"), ("monthly_sales_target", "double"),
    ],
    "products": [
        ("product_id", "id"), ("model", "text"), ("variant", "text"), ("segment", "text"),
        ("fuel_type", "text"), ("list_price", "double"), ("unit_cost", "double"),
    ],
    "sales": [
        ("sale_id", "id"), ("sale_date", "date"), ("dealership_id", "id"), ("product_id", "id"),
        ("quantity", "int"), ("unit_price", "double"), ("discount_pct", "double"),
        ("payment_type", "text"), ("customer_type", "text"),
    ],
    "inventory": [
        ("snapshot_date", "date"), ("dealership_id", "id"), ("product_id", "id"),
        ("units_on_hand", "int"), ("units_received", "int"),
    ],
}
RAW_COLUMNS: dict[str, list[str]] = {e: [f for f, _ in fields] for e, fields in FIELDS.items()}
KEYS: dict[str, list[str]] = {
    "dealerships": ["dealership_id"],
    "products": ["product_id"],
    "sales": ["sale_id"],
    "inventory": ["snapshot_date", "dealership_id", "product_id"],
}
PART_COL: dict[str, str | None] = {"dealerships": None, "products": None, "sales": "sale_month", "inventory": "snapshot_month"}
DATE_COL: dict[str, str | None] = {"dealerships": None, "products": None, "sales": "sale_date", "inventory": "snapshot_date"}
PAYLOAD_COLS: dict[str, list[str]] = {
    "dealerships": ["dealership_name", "city", "state", "region", "opened_date", "monthly_sales_target"],
    "products": ["model", "variant", "segment", "fuel_type", "list_price", "unit_cost"],
    "sales": ["sale_date", "dealership_id", "product_id", "quantity", "unit_price", "discount_pct", "payment_type", "customer_type"],
    "inventory": ["units_on_hand", "units_received"],
}
PROCESS_ORDER = ("dealerships", "products", "sales", "inventory")  # references first (orphan checks need them)
META_COLS = ["_batch_id", "_ingested_at", "_source_file", "_row_id"]


@dataclass(frozen=True)
class Rule:
    """A data quality rule. ``failed`` is True when the record VIOLATES the rule."""

    name: str
    severity: str  # "error" | "warning"
    failed: Column
    description: str


def _blank(raw_col: str) -> Column:
    return F.trim(F.coalesce(F.col(raw_col), F.lit(""))) == ""


def _clean(name: str) -> Column:
    return F.when(F.trim(F.col(name)) == "", F.lit(None).cast("string")).otherwise(F.trim(F.col(name)))


def typed_expr(name: str, kind: str) -> Column:
    """Typed column expression for one raw text field. Unparseable values become NULL."""
    if kind == "id":
        return F.upper(_clean(name)).alias(name)
    if kind == "text":
        return _clean(name).alias(name)
    if kind == "date":
        return F.to_date(_clean(name), "yyyy-MM-dd").alias(name)
    as_double = _clean(name).cast("double")
    if kind == "double":
        return F.when(F.isnan(as_double), F.lit(None).cast("double")).otherwise(as_double).alias(name)
    if kind == "int":  # must be a whole number: "2.5" is invalid, not truncated to 2
        return F.when(as_double == F.floor(as_double), as_double.cast("int")).alias(name)
    raise ValueError(f"Unknown kind {kind}")


def to_typed(bronze: DataFrame, entity: str) -> DataFrame:
    """Raw text kept as ``raw_<col>``; typed columns added under the canonical names."""
    raw = [F.col(c).alias(f"raw_{c}") for c in RAW_COLUMNS[entity]]
    typed = [typed_expr(name, kind) for name, kind in FIELDS[entity]]
    return bronze.select(*[F.col(c) for c in META_COLS], *raw, *typed)


# --------------------------------------------------------------------------------------------------
# Rule builders
# --------------------------------------------------------------------------------------------------
def _required_text(field: str, pattern: str | None = None) -> list[Rule]:
    rules = [Rule(f"missing_{field}", "error", _blank(f"raw_{field}"), f"{field} is null/blank")]
    if pattern:
        rules.append(
            Rule(f"invalid_{field}_format", "error",
                 F.col(field).isNotNull() & ~F.col(field).rlike(pattern), f"{field} must match {pattern}")
        )
    return rules


def _numeric(field: str, required: bool, lo: float | None, hi: float | None, lo_inclusive: bool = True) -> list[Rule]:
    raw = f"raw_{field}"
    rules = [
        Rule(f"missing_{field}", "error" if required else "warning", _blank(raw), f"{field} is null/blank"),
        Rule(f"invalid_{field}", "error", ~_blank(raw) & F.col(field).isNull(), f"{field} not a valid number"),
    ]
    out = F.lit(False)
    if lo is not None:
        out = out | ((F.col(field) < F.lit(lo)) if lo_inclusive else (F.col(field) <= F.lit(lo)))
    if hi is not None:
        out = out | (F.col(field) > F.lit(hi))
    rules.append(Rule(f"out_of_range_{field}", "error", F.col(field).isNotNull() & out, f"{field} outside allowed range"))
    return rules


def _date(field: str, cfg: dict[str, Any], not_future: bool = True, min_date: str | None = None) -> list[Rule]:
    raw = f"raw_{field}"
    rules = [
        Rule(f"missing_{field}", "error", _blank(raw), f"{field} is null/blank"),
        Rule(f"invalid_{field}", "error", ~_blank(raw) & F.col(field).isNull(), f"{field} not a valid yyyy-MM-dd date"),
    ]
    if min_date:
        rules.append(Rule(f"{field}_too_old", "error", F.col(field) < F.lit(min_date).cast("date"), f"{field} before {min_date}"))
    if not_future:
        rules.append(Rule(f"{field}_in_future", "error", F.col(field) > F.current_date(), f"{field} is in the future"))
    return rules


def _allowed(field: str, allowed: list[str], required: bool = True) -> list[Rule]:
    rules = [
        Rule(f"invalid_{field}", "error", F.col(field).isNotNull() & ~F.col(field).isin(*allowed), f"{field} not in {allowed}")
    ]
    rules.append(Rule(f"missing_{field}", "error" if required else "warning", _blank(f"raw_{field}"), f"{field} null/blank"))
    return rules


def build_rules(cfg: dict[str, Any], entity: str) -> list[Rule]:
    """Rule list for one entity, parameterised from configuration."""
    q = cfg["quality"]
    if entity == "dealerships":
        return [
            *_required_text("dealership_id", r"^D[0-9]{3}$"),
            *_required_text("dealership_name"),
            *_required_text("region"),
            Rule("invalid_opened_date", "error", ~_blank("raw_opened_date") & F.col("opened_date").isNull(), "opened_date invalid"),
            *_numeric("monthly_sales_target", True, 0, None, lo_inclusive=False),
        ]
    if entity == "products":
        return [
            *_required_text("product_id", r"^P[0-9]{3}$"),
            *_required_text("model"),
            *_allowed("segment", q["allowed_segments"]),
            *_allowed("fuel_type", q["allowed_fuel_types"]),
            *_numeric("list_price", True, 0, None, lo_inclusive=False),
            *_numeric("unit_cost", True, 0, None, lo_inclusive=False),
            Rule("cost_not_below_price", "error", F.col("unit_cost") >= F.col("list_price"), "unit_cost must be below list_price"),
        ]
    if entity == "sales":
        return [
            *_required_text("sale_id", r"^S[0-9]{7}$"),
            *_date("sale_date", cfg, min_date=q["min_sale_date"]),
            *_required_text("dealership_id"),
            Rule("orphan_dealership", "error", F.col("dealership_id").isNotNull() & F.col("_dealer_ok").isNull(), "unknown dealership_id"),
            *_required_text("product_id"),
            Rule("orphan_product", "error", F.col("product_id").isNotNull() & F.col("_product_ok").isNull(), "unknown product_id"),
            *_numeric("quantity", True, 1, q["max_quantity_per_sale"]),
            *_numeric("unit_price", True, 0, q["max_unit_price"], lo_inclusive=False),
            *_numeric("discount_pct", False, 0, q["max_discount_pct"]),  # missing => imputed as 0, with a warning
            *_allowed("payment_type", q["allowed_payment_types"]),
            *_allowed("customer_type", q["allowed_customer_types"], required=False),  # missing => 'Unknown'
        ]
    if entity == "inventory":
        return [
            *_date("snapshot_date", cfg, min_date=q["min_sale_date"]),
            *_required_text("dealership_id"),
            Rule("orphan_dealership", "error", F.col("dealership_id").isNotNull() & F.col("_dealer_ok").isNull(), "unknown dealership_id"),
            *_required_text("product_id"),
            Rule("orphan_product", "error", F.col("product_id").isNotNull() & F.col("_product_ok").isNull(), "unknown product_id"),
            *_numeric("units_on_hand", True, 0, None),
            *_numeric("units_received", True, 0, None),
        ]
    raise ValueError(f"Unknown entity {entity}")


def apply_rules(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Evaluate every rule once; add ``_dq_errors`` / ``_dq_warnings`` (arrays of failed rule names)."""

    def names(severity: str) -> Column:
        exprs = [F.when(F.coalesce(r.failed, F.lit(False)), F.lit(r.name)) for r in rules if r.severity == severity]
        arr = F.array(*exprs).cast("array<string>") if exprs else F.array().cast("array<string>")
        return F.filter(arr, lambda x: x.isNotNull())

    return df.withColumn("_dq_errors", names("error")).withColumn("_dq_warnings", names("warning"))
