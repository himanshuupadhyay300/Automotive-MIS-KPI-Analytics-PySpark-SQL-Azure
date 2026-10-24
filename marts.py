"""Gold layer, part 2: KPI marts defined as plain SQL files (sql/marts/*.sql).

Each file holds ONE SELECT over the star schema; the file name becomes the Gold table name
(sql/marts/mart_monthly_sales.sql -> gold/mart_monthly_sales). Marts are small, so each run fully recomputes
them (overwrite) from the star schema - trivially idempotent.
"""
from __future__ import annotations

from pathlib import Path
from string import Template
from typing import Any

from pyspark.sql import SparkSession

from src.utils import storage
from src.utils.config import table_path
from src.utils.exceptions import PipelineError
from src.utils.logging_utils import get_logger

log = get_logger(__name__)
STAR_TABLES = ("dim_date", "dim_dealership", "dim_product", "fact_sales", "fact_inventory_snapshot")


def register_star_views(spark: SparkSession, cfg: dict[str, Any]) -> None:
    """Expose the star schema as temp views so the SQL files read like normal warehouse SQL."""
    fmt = cfg["storage"]["format"]
    for name in STAR_TABLES:
        path = table_path(cfg, "gold", name)
        if not storage.table_exists(spark, path, fmt):
            raise PipelineError(f"Star table missing: {name} (run the gold star build first)")
        storage.read_table(spark, path, fmt).createOrReplaceTempView(name)


def run_marts(spark: SparkSession, cfg: dict[str, Any]) -> dict[str, int]:
    """Execute every mart SQL file and write the result to Gold. Returns row counts per mart."""
    sql_dir = Path(cfg["marts"]["sql_dir"])
    files = sorted(sql_dir.glob("mart_*.sql"))
    if not files:
        raise PipelineError(f"No mart_*.sql files in {sql_dir}")
    register_star_views(spark, cfg)
    params = {"abc_a": cfg["gold"]["abc_a_threshold"], "abc_b": cfg["gold"]["abc_b_threshold"]}
    fmt = cfg["storage"]["format"]
    counts: dict[str, int] = {}
    for sql_file in files:
        statement = Template(sql_file.read_text(encoding="utf-8")).safe_substitute(params)
        df = spark.sql(statement).cache()
        counts[sql_file.stem] = df.count()
        storage.overwrite_table(df, table_path(cfg, "gold", sql_file.stem), fmt)
        df.unpersist()
    log.info("marts complete", extra={"ctx": counts})
    return counts
