"""Silver layer: typing, validation, quarantine, de-duplication and idempotent upsert for every entity.

For each entity in a Bronze batch:
    typed -> rules -> valid / rejected
      valid    -> de-dupe in batch -> compare with Silver (already seen? changed? late?) -> upsert on business key
      rejected -> rejects table (raw values + failed rule names), replaced per batch id (re-run safe)

Reference entities (dealerships, products) are tiny: they are merged in full (SCD Type 1, latest wins).
Transactional entities (sales, inventory) are upserted partition-wise by month.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.transformations.dedupe import deduplicate_latest
from src.utils import storage
from src.utils.config import table_path
from src.utils.logging_utils import get_logger
from src.validation.entities import (
    DATE_COL,
    FIELDS,
    KEYS,
    META_COLS,
    PART_COL,
    PAYLOAD_COLS,
    RAW_COLUMNS,
    apply_rules,
    build_rules,
    to_typed,
)

log = get_logger(__name__)
ORDER_COLS = ["_ingested_at", "_source_file", "_row_id"]  # latest ingested copy wins; deterministic ties


def _count_by_rule(df: DataFrame, col: str) -> dict[str, int]:
    rows = df.select(F.explode(col).alias("rule")).groupBy("rule").count().collect()
    return {r["rule"]: int(r["count"]) for r in rows}


def _ids(spark: SparkSession, cfg: dict[str, Any], entity: str, id_col: str, flag: str) -> DataFrame | None:
    path = table_path(cfg, "silver", entity)
    if not storage.table_exists(spark, path, cfg["storage"]["format"]):
        return None
    return storage.read_table(spark, path, cfg["storage"]["format"]).select(id_col).distinct().withColumn(flag, F.lit(True))


def _attach_reference_flags(spark: SparkSession, cfg: dict[str, Any], typed: DataFrame) -> DataFrame:
    """Add _dealer_ok / _product_ok (NULL => unknown id) for the referential-integrity rules."""
    for entity, id_col, flag in (("dealerships", "dealership_id", "_dealer_ok"), ("products", "product_id", "_product_ok")):
        ids = _ids(spark, cfg, entity, id_col, flag)
        typed = typed.join(F.broadcast(ids), id_col, "left") if ids is not None else typed.withColumn(flag, F.lit(None).cast("boolean"))
    return typed


def _project(valid: DataFrame, entity: str) -> DataFrame:
    """Silver column set: typed fields (optional values imputed), derived partition column, payload hash.

    The hash is computed on the *imputed* values so that two logically equal events hash equally.
    """
    cols: list[Any] = []
    for name, _ in FIELDS[entity]:
        if entity == "sales" and name == "discount_pct":
            cols.append(F.coalesce(F.col(name), F.lit(0.0)).alias(name))  # documented imputation: no discount
        elif entity == "sales" and name == "customer_type":
            cols.append(F.coalesce(F.col(name), F.lit("Unknown")).alias(name))
        else:
            cols.append(F.col(name))
    if PART_COL[entity]:
        cols.append(F.date_format(DATE_COL[entity], "yyyy-MM").alias(PART_COL[entity]))
    out = valid.select(*cols, F.col("_dq_warnings"), *[F.col(c) for c in META_COLS])
    payload = F.concat_ws("|", *[F.col(c).cast("string") for c in PAYLOAD_COLS[entity]])
    return out.withColumn("_payload_hash", F.sha2(payload, 256))


def _rejects(rejected: DataFrame, entity: str) -> DataFrame:
    return rejected.select(
        *[F.col(c) for c in META_COLS],
        *[f"raw_{c}" for c in RAW_COLUMNS[entity]],
        "_dq_errors",
        F.concat_ws(",", "_dq_errors").alias("_dq_error_list"),
    )


def process_entity(spark: SparkSession, cfg: dict[str, Any], entity: str, bronze_df: DataFrame, batch_id: str) -> dict[str, Any]:
    """Validate, de-duplicate and merge one entity's Bronze batch into Silver; return DQ metrics."""
    fmt = cfg["storage"]["format"]
    silver_path = table_path(cfg, "silver", entity)
    keys = KEYS[entity]
    date_col, part_col = DATE_COL[entity], PART_COL[entity]

    typed = to_typed(bronze_df, entity)
    if entity in ("sales", "inventory"):
        typed = _attach_reference_flags(spark, cfg, typed)
    validated = apply_rules(typed, build_rules(cfg, entity)).cache()
    bronze_rows = validated.count()
    is_valid = F.size("_dq_errors") == 0
    valid, rejected = validated.filter(is_valid), validated.filter(~is_valid)
    rejected_rows = rejected.count()
    rejected_by_rule = _count_by_rule(rejected, "_dq_errors")
    warnings_by_rule = _count_by_rule(valid, "_dq_warnings")
    rows_with_warnings = valid.filter(F.size("_dq_warnings") > 0).count()

    candidates = _project(valid, entity).cache()
    valid_rows = candidates.count()
    unique_keys = candidates.select(*keys).distinct().count()
    conflicting = (
        candidates.groupBy(*keys).agg(F.countDistinct("_payload_hash").alias("n")).filter("n > 1").count()
    )
    deduped = deduplicate_latest(candidates, keys, ORDER_COLS)

    matched = changed = late = 0
    if storage.table_exists(spark, silver_path, fmt):
        existing = storage.read_table(spark, silver_path, fmt)
        known = existing.select(*keys, F.col("_payload_hash").alias("_existing_hash"))
        enriched = deduped.join(known, keys, "left")
        enriched = enriched.withColumn("_matched", F.col("_existing_hash").isNotNull()).withColumn(
            "_changed", F.col("_existing_hash").isNotNull() & (F.col("_existing_hash") != F.col("_payload_hash"))
        )
        if date_col:
            wm = existing.groupBy("dealership_id").agg(F.max(date_col).alias("_watermark"))
            enriched = enriched.join(F.broadcast(wm), "dealership_id", "left").withColumn(
                "is_late_arrival", (~F.col("_matched")) & F.col("_watermark").isNotNull() & (F.col(date_col) < F.col("_watermark"))
            ).drop("_watermark")
    else:
        enriched = deduped.withColumn("_matched", F.lit(False)).withColumn("_changed", F.lit(False))
        if date_col:
            enriched = enriched.withColumn("is_late_arrival", F.lit(False))
    enriched = enriched.cache()
    matched, changed = enriched.filter("_matched").count(), enriched.filter("_changed").count()
    if date_col:
        late = enriched.filter("is_late_arrival").count()
    final = enriched.drop("_existing_hash", "_matched", "_changed").withColumn("_processed_at", F.current_timestamp()).cache()
    silver_rows_written = final.count()
    affected_months = sorted({str(r[0]) for r in final.select(part_col).distinct().collect()}) if part_col else []

    if part_col:
        storage.upsert(spark, final, silver_path, fmt, keys, ORDER_COLS, part_col)
    elif silver_rows_written:
        _upsert_reference(spark, final, silver_path, fmt, keys)
    if rejected_rows:
        storage.replace_partitions(spark, _rejects(rejected, entity), table_path(cfg, "rejects", entity), fmt, "_batch_id", [batch_id])
    silver_rows_after = storage.read_table(spark, silver_path, fmt).count() if storage.table_exists(spark, silver_path, fmt) else 0
    for df in (validated, candidates, enriched, final):
        df.unpersist()

    metrics = {
        "entity": entity,
        "bronze_rows": bronze_rows,
        "valid_rows": valid_rows,
        "rejected_rows": rejected_rows,
        "rows_with_warnings": rows_with_warnings,
        "rejected_by_rule": rejected_by_rule,
        "warnings_by_rule": warnings_by_rule,
        "in_batch_duplicate_rows_removed": valid_rows - unique_keys,
        "conflicting_duplicate_keys": conflicting,
        "rows_matching_existing_silver": matched,
        "rows_changing_existing_silver": changed,
        "late_arrival_rows": late,
        "silver_rows_written": silver_rows_written,
        "silver_rows_after": silver_rows_after,
        "affected_months": affected_months,
    }
    log.info("silver entity complete", extra={"ctx": {"entity": entity, "batch_id": batch_id, "valid": valid_rows, "rejected": rejected_rows}})
    return metrics


def _upsert_reference(spark: SparkSession, new_df: DataFrame, path: str, fmt: str, keys: list[str]) -> None:
    """Merge a small reference table in full (SCD Type 1): latest ingested version of each key wins."""
    if not storage.table_exists(spark, path, fmt):
        storage.overwrite_table(new_df, path, fmt)
        return
    existing = storage.read_table(spark, path, fmt)
    combined = deduplicate_latest(existing.unionByName(new_df, allowMissingColumns=True), keys, ORDER_COLS)
    combined = combined.localCheckpoint(eager=True)  # materialise before overwriting the source files
    storage.overwrite_table(combined, path, fmt)


def run_silver_for_batch(spark: SparkSession, cfg: dict[str, Any], batch_id: str, entities: list[str]) -> dict[str, Any]:
    """Process the entities of one batch in dependency order. Returns a batch-level DQ report dict."""
    fmt = cfg["storage"]["format"]
    report: dict[str, Any] = {"batch_id": batch_id, "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "entities": {}}
    for entity in entities:
        bronze = storage.read_table(spark, table_path(cfg, "bronze", entity), fmt).filter(F.col("_batch_id") == batch_id)
        report["entities"][entity] = process_entity(spark, cfg, entity, bronze, batch_id)
    return report
