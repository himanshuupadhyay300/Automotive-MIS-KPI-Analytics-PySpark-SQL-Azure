"""Pipeline orchestrator (CLI): Bronze -> Silver -> Gold star schema -> KPI marts.

    python -m src.pipeline                              # all stages, every new file
    python -m src.pipeline --only "*_001.csv"           # ingest only files whose name matches (repeatable)
    python -m src.pipeline --stage silver               # one stage; resumes any pending batch
    python -m src.pipeline --format delta               # Delta Lake instead of Parquet
"""
from __future__ import annotations

import argparse
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession

from src.ingestion.bronze_ingest import ingest_to_bronze
from src.transformations.gold_star import run_gold_star
from src.transformations.marts import run_marts
from src.transformations.silver import run_silver_for_batch
from src.utils import storage
from src.utils.config import load_config, state_file_path, table_path
from src.utils.exceptions import PipelineError
from src.utils.logging_utils import configure_logging, get_logger
from src.utils.spark_session import get_spark
from src.utils.state import StateStore
from src.validation.entities import PROCESS_ORDER
from src.validation.quality_report import render_dq_markdown, render_summary_markdown, write_report

log = get_logger(__name__)
STAGE_CHOICES = ("all", "bronze", "silver", "gold")
GOLD_TABLES = (
    "dim_date", "dim_dealership", "dim_product", "fact_sales", "fact_inventory_snapshot",
    "mart_monthly_sales", "mart_dealership_monthly", "mart_product_kpis", "mart_inventory_weekly",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def table_counts(spark: SparkSession, cfg: dict[str, Any]) -> dict[str, int]:
    """Row count of every lakehouse table that exists."""
    fmt = cfg["storage"]["format"]
    targets = [(f"{layer}_{e}", table_path(cfg, layer, e)) for layer in ("bronze", "silver", "rejects") for e in PROCESS_ORDER]
    targets += [(t, table_path(cfg, "gold", t)) for t in GOLD_TABLES]
    return {name: storage.read_table(spark, p, fmt).count() for name, p in targets if storage.table_exists(spark, p, fmt)}


def run_pipeline(
    cfg: dict[str, Any], stage: str = "all", only: list[str] | None = None, spark: SparkSession | None = None
) -> dict[str, Any]:
    """Run the requested stage(s); persist and return the execution summary (status failed + PipelineError on error)."""
    if stage not in STAGE_CHOICES:
        raise PipelineError(f"Unknown stage {stage!r}; choose from {STAGE_CHOICES}")
    run_id = f"run_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"
    started = time.time()
    summary: dict[str, Any] = {
        "run_id": run_id, "status": "running", "storage_format": cfg["storage"]["format"],
        "started_at": _now(), "stages": {}, "dq_reports": [],
    }
    spark = spark or get_spark(cfg)
    state = StateStore(state_file_path(cfg))
    reports_dir = cfg["storage"]["reports_path"]

    def wanted(name: str) -> bool:
        return stage in ("all", name)

    try:
        if wanted("bronze"):
            t0 = time.time()
            res = ingest_to_bronze(spark, cfg, state, only=only)
            summary["stages"]["bronze"] = {
                "batch_id": res.batch_id, "files_ingested": res.files_ingested,
                "files_skipped_already_ingested": res.files_skipped, "rows_by_entity": res.rows_by_entity,
                "seconds": round(time.time() - t0, 2),
            }

        if wanted("silver"):
            t0 = time.time()
            batches = state.pending_batches("silver")
            totals = {"valid_rows": 0, "rejected_rows": 0, "late_arrival_rows": 0}
            for batch_id in batches:
                entities = [e for e in PROCESS_ORDER if e in state.get_batch(batch_id).get("entity_rows", {})]
                report = run_silver_for_batch(spark, cfg, batch_id, entities)
                paths = write_report(report, reports_dir, f"dq_report_{batch_id}", render_dq_markdown(report))
                summary["dq_reports"].append(paths["markdown"])
                ent = report["entities"]
                state.mark_stage_done(
                    batch_id, "silver",
                    sales_months=ent.get("sales", {}).get("affected_months", []),
                    inventory_months=ent.get("inventory", {}).get("affected_months", []),
                    products_changed=bool(ent.get("products", {}).get("silver_rows_written", 0)),
                )
                for m in ent.values():
                    for k in totals:
                        totals[k] += m[k]
            summary["stages"]["silver"] = {"batches_processed": len(batches), **totals, "seconds": round(time.time() - t0, 2)}

        if wanted("gold"):
            t0 = time.time()
            pending = state.pending_batches("gold")
            result: dict[str, Any] = {}
            if pending:
                infos = [state.get_batch(b) for b in pending]
                sales_m = sorted({m for i in infos for m in i.get("sales_months", [])})
                inv_m = sorted({m for i in infos for m in i.get("inventory_months", [])})
                rebuild_all = any(i.get("products_changed") for i in infos)  # product cost feeds the fact measures
                result = run_gold_star(spark, cfg, sales_m, inv_m, rebuild_all)
                result["marts"] = run_marts(spark, cfg) if result else {}
                for b in pending:
                    state.mark_stage_done(b, "gold")
            summary["stages"]["gold"] = {"batches_processed": len(pending), **result, "seconds": round(time.time() - t0, 2)}

        summary["table_row_counts"] = table_counts(spark, cfg)
        summary["status"] = "success"
    except Exception as exc:  # noqa: BLE001 - recorded in the summary, re-raised as PipelineError
        summary["status"] = "failed"
        summary["error"] = f"{type(exc).__name__}: {exc}"
        log.exception("pipeline failed")
        raise PipelineError(summary["error"]) from exc
    finally:
        summary["finished_at"] = _now()
        summary["duration_seconds"] = round(time.time() - started, 2)
        paths = write_report(summary, reports_dir, f"pipeline_summary_{run_id}", render_summary_markdown(summary))
        summary["summary_report"] = paths["markdown"]
        log.info("pipeline finished", extra={"ctx": {"status": summary["status"], "report": paths["markdown"]}})
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Automotive MIS & KPI analytics pipeline")
    p.add_argument("--config")
    p.add_argument("--stage", choices=STAGE_CHOICES, default="all")
    p.add_argument("--only", action="append", help="only ingest files whose NAME matches this glob (repeatable)")
    p.add_argument("--format", choices=("parquet", "delta"))
    p.add_argument("--input-path")
    p.add_argument("--base-path")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    overrides: dict[str, Any] = {"storage": {}}
    for arg, key in ((args.format, "format"), (args.input_path, "input_path"), (args.base_path, "base_path")):
        if arg:
            overrides["storage"][key] = arg
    try:
        cfg = load_config(args.config, overrides)
        configure_logging(cfg.get("logging", {}).get("level", "INFO"), cfg.get("logging", {}).get("json", True))
        summary = run_pipeline(cfg, stage=args.stage, only=args.only)
    except PipelineError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"\nPipeline {summary['status']}. Summary: {summary['summary_report']}")
    for path in summary["dq_reports"]:
        print(f"Data quality report: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
