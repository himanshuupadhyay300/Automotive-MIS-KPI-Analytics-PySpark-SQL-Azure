.PHONY: install sample-data test lint run batch1 batch2 sql delta clean

PY ?= python

install:
	$(PY) -m pip install -r requirements-dev.txt

sample-data:
	$(PY) -m src.ingestion.generate_synthetic_data --output data/sample --seed 42

test:
	$(PY) -m pytest

lint:
	$(PY) -m ruff check .

# Incremental-load demo: batch 1 first, then batch 2 arrives (late sales, corrections, duplicates)
batch1:
	$(PY) -m src.pipeline --only "*_001.csv"

batch2:
	$(PY) -m src.pipeline

run: batch1 batch2

sql:
	$(PY) -m src.utils.sql_runner sql/analysis_queries.sql

# Same pipeline on Delta Lake (needs internet once, to fetch Delta jars)
delta:
	$(PY) -m src.pipeline --format delta --base-path output/lakehouse_delta

clean:
	rm -rf output spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
