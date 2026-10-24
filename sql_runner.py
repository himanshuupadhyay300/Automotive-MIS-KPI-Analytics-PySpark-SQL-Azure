"""Run an analytical SQL file against the local lakehouse.

    python -m src.utils.sql_runner sql/analysis_queries.sql

Queries are separated by ``-- name: <title>`` comment lines. Every Gold table is a temp view under its own name,
Silver tables are ``silver_<entity>`` and quarantine tables ``rejects_<entity>``.
"""
from __future__ import annotations

import re
import sys

from src.pipeline import GOLD_TABLES
from src.utils import storage
from src.utils.config import load_config, table_path
from src.utils.exceptions import PipelineError
from src.utils.spark_session import get_spark
from src.validation.entities import PROCESS_ORDER


def split_queries(sql_text: str) -> list[tuple[str, str]]:
    """Split a SQL file into (name, statement) pairs using ``-- name:`` markers."""
    parts = re.split(r"^--\s*name:\s*(.+)$", sql_text, flags=re.MULTILINE)
    return [(parts[i].strip(), parts[i + 1].strip().rstrip(";").strip()) for i in range(1, len(parts), 2) if parts[i + 1].strip()]


def register_views(spark, cfg) -> None:
    fmt = cfg["storage"]["format"]
    views = {t: ("gold", t) for t in GOLD_TABLES}
    views.update({f"silver_{e}": ("silver", e) for e in PROCESS_ORDER})
    views.update({f"rejects_{e}": ("rejects", e) for e in PROCESS_ORDER})
    for view, (layer, name) in views.items():
        path = table_path(cfg, layer, name)
        if storage.table_exists(spark, path, fmt):
            storage.read_table(spark, path, fmt).createOrReplaceTempView(view)


def main(sql_file: str, config_path: str | None = None) -> int:
    cfg = load_config(config_path)
    spark = get_spark(cfg)
    register_views(spark, cfg)
    queries = split_queries(open(sql_file, encoding="utf-8").read())
    if not queries:
        raise PipelineError(f"No '-- name:' queries found in {sql_file}")
    for name, statement in queries:
        print(f"\n=== {name} ===")
        spark.sql(statement).show(20, truncate=False)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "sql/analysis_queries.sql"))
