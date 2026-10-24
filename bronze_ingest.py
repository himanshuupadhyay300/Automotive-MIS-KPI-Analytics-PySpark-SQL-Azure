"""Bronze layer: land raw CSV files for every entity exactly as received, plus lineage metadata.

* Everything is stored as STRING so bad values are never coerced or lost.
* Files are identified by SHA-256: an already-ingested file (even if renamed) is skipped -> idempotent.
* One run = one batch id spanning all new files of all entities.
* The state file is updated only AFTER the Bronze writes succeed (a crash in between re-ingests the file;
  Silver de-duplicates, so results stay correct).
"""
from __future__ import annotations

import fnmatch
import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils import storage
from src.utils.config import table_path, to_spark_path
from src.utils.exceptions import IngestionError
from src.utils.logging_utils import get_logger
from src.utils.state import StateStore
from src.validation.entities import PROCESS_ORDER, RAW_COLUMNS

log = get_logger(__name__)


@dataclass
class BronzeResult:
    batch_id: str | None
    files_ingested: int
    files_skipped: int
    rows_by_entity: dict[str, int] = field(default_factory=dict)

    @property
    def rows_ingested(self) -> int:
        return sum(self.rows_by_entity.values())


def file_sha256(path: Path, chunk_size: int = 1 << 20) -> str:
    """Streaming SHA-256 of a file."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def discover_files(input_dir: str | Path, patterns: list[str]) -> list[Path]:
    """Sorted, non-recursive glob of ``input_dir`` for any of ``patterns``."""
    folder = Path(input_dir)
    if not folder.is_dir():
        raise IngestionError(f"Input folder not found: {folder}")
    found: set[Path] = set()
    for pattern in patterns:
        found.update(p for p in folder.glob(pattern) if p.is_file())
    return sorted(found)


def ingest_to_bronze(
    spark: SparkSession,
    cfg: dict[str, Any],
    state: StateStore,
    only: list[str] | None = None,
    batch_id: str | None = None,
) -> BronzeResult:
    """Ingest every not-yet-seen file (optionally restricted to names matching ``only`` globs)."""
    fmt = cfg["storage"]["format"]
    input_dir = cfg["storage"]["input_path"]
    pending: dict[str, list[dict[str, str]]] = {}
    seen: set[str] = set()
    skipped = 0

    for entity in PROCESS_ORDER:
        for path in discover_files(input_dir, cfg["ingestion"]["entities"][entity]):
            if only and not any(fnmatch.fnmatch(path.name, pat) for pat in only):
                continue
            digest = file_sha256(path)
            if state.has_file(digest) or digest in seen:
                skipped += 1
                log.info("skipping already-ingested file", extra={"ctx": {"file": path.name}})
                continue
            seen.add(digest)
            pending.setdefault(entity, []).append({"path": str(path), "sha256": digest})

    if not pending:
        log.info("bronze: nothing new", extra={"ctx": {"files_skipped": skipped}})
        return BronzeResult(None, 0, skipped)

    batch_id = batch_id or f"batch_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    rows_by_entity: dict[str, int] = {}

    for entity, files in pending.items():
        df = (
            spark.read.option("header", "true").option("mode", "PERMISSIVE").option("inferSchema", "false")
            .csv([to_spark_path(f["path"]) for f in files])
        )
        missing = [c for c in RAW_COLUMNS[entity] if c not in df.columns]
        if missing:
            raise IngestionError(f"{entity}: file(s) missing required columns {missing}: {[Path(f['path']).name for f in files]}")
        bronze = (
            df.select(*[F.col(c).cast("string") for c in RAW_COLUMNS[entity]])
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_row_id", F.monotonically_increasing_id())
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
            .withColumn("_ingest_date", F.to_date("_ingested_at").cast("string"))
        ).cache()
        rows_by_entity[entity] = bronze.count()
        storage.append(bronze, table_path(cfg, "bronze", entity), fmt, partition_by=["_ingest_date"])
        bronze.unpersist()

    all_files = [f for files in pending.values() for f in files]
    state.register_batch(batch_id, all_files, sum(rows_by_entity.values()))
    state.mark_stage_done(batch_id, "bronze", entity_rows=rows_by_entity)
    log.info("bronze complete", extra={"ctx": {"batch_id": batch_id, "files": len(all_files), "rows": rows_by_entity}})
    return BronzeResult(batch_id, len(all_files), skipped, rows_by_entity)
