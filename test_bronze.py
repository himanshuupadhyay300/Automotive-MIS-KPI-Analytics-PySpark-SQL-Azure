import os
import shutil
from pathlib import Path

import pytest

from src.ingestion.bronze_ingest import discover_files, file_sha256, ingest_to_bronze
from src.utils import storage
from src.utils.config import state_file_path, table_path
from src.utils.exceptions import IngestionError
from src.utils.state import StateStore

SALES_HEADER = "sale_id,sale_date,dealership_id,product_id,quantity,unit_price,discount_pct,payment_type,customer_type\n"
SALE = "S0000001,2025-03-10,D001,P001,1,500000,abc,Loan,Retail\n"


def write(cfg, name, text):
    folder = cfg["storage"]["input_path"]
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, name)
    Path(path).write_text(text, encoding="utf-8")
    return path


def test_discover_and_hash(cfg):
    a = write(cfg, "sales_a.csv", SALES_HEADER)
    write(cfg, "notes.txt", "x")
    assert [p.name for p in discover_files(cfg["storage"]["input_path"], ["sales_*.csv"])] == ["sales_a.csv"]
    assert len(file_sha256(Path(a))) == 64


def test_missing_input_folder(cfg):
    with pytest.raises(IngestionError):
        discover_files(cfg["storage"]["input_path"], ["*.csv"])


def test_ingest_keeps_raw_text_and_lineage(spark, cfg):
    write(cfg, "sales_a.csv", SALES_HEADER + SALE)
    res = ingest_to_bronze(spark, cfg, StateStore(state_file_path(cfg)))
    assert res.rows_by_entity == {"sales": 1} and res.files_ingested == 1
    row = storage.read_table(spark, table_path(cfg, "bronze", "sales"), "parquet").first()
    assert row["discount_pct"] == "abc"  # not coerced
    assert row["_batch_id"] == res.batch_id and row["_source_file"].endswith("sales_a.csv")


def test_renamed_copy_is_skipped(spark, cfg):
    path = write(cfg, "sales_a.csv", SALES_HEADER + SALE)
    ingest_to_bronze(spark, cfg, StateStore(state_file_path(cfg)))
    shutil.copy(path, path.replace("sales_a", "sales_b"))
    again = ingest_to_bronze(spark, cfg, StateStore(state_file_path(cfg)))
    assert (again.files_ingested, again.files_skipped, again.batch_id) == (0, 2, None)
    assert storage.read_table(spark, table_path(cfg, "bronze", "sales"), "parquet").count() == 1


def test_only_filter_restricts_files(spark, cfg):
    write(cfg, "sales_001.csv", SALES_HEADER + SALE)
    write(cfg, "sales_002.csv", SALES_HEADER + SALE.replace("S0000001", "S0000002"))
    res = ingest_to_bronze(spark, cfg, StateStore(state_file_path(cfg)), only=["*_001.csv"])
    assert res.files_ingested == 1 and res.rows_ingested == 1


def test_missing_required_column_fails_fast(spark, cfg):
    write(cfg, "sales_bad.csv", "sale_id,sale_date\nS1,2025-01-01\n")
    with pytest.raises(IngestionError, match="missing required columns"):
        ingest_to_bronze(spark, cfg, StateStore(state_file_path(cfg)))
