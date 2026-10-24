"""Configuration loading, environment overrides and validation (no secrets live in config)."""
from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any

import yaml

from src.utils.exceptions import ConfigError

DEFAULT_CONFIG_PATH = Path("config/pipeline_config.yaml")
ENTITY_NAMES = ("dealerships", "products", "sales", "inventory")
SUPPORTED_FORMATS = {"parquet", "delta"}

ENV_OVERRIDES: dict[str, tuple[str, str]] = {
    "MIS_STORAGE_FORMAT": ("storage", "format"),
    "MIS_BASE_PATH": ("storage", "base_path"),
    "MIS_INPUT_PATH": ("storage", "input_path"),
    "MIS_REPORTS_PATH": ("storage", "reports_path"),
    "MIS_LOG_LEVEL": ("logging", "level"),
}


def load_config(path: str | Path | None = None, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Load YAML, apply env + explicit overrides (highest priority), validate, return."""
    cfg_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not cfg_path.is_file():
        raise ConfigError(f"Config file not found: {cfg_path}")
    try:
        cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {cfg_path}: {exc}") from exc
    if not isinstance(cfg, dict):
        raise ConfigError(f"Config root must be a mapping: {cfg_path}")
    for env_name, (section, key) in ENV_OVERRIDES.items():
        if os.environ.get(env_name):
            cfg.setdefault(section, {})[key] = os.environ[env_name]
    if overrides:
        cfg = _deep_merge(cfg, overrides)
    validate_config(cfg)
    return cfg


def _deep_merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def validate_config(cfg: dict[str, Any]) -> None:
    """Fail fast with a readable message when the configuration is unusable."""
    for section in ("storage", "ingestion", "quality", "gold", "spark", "marts"):
        if not isinstance(cfg.get(section), dict):
            raise ConfigError(f"Missing config section: '{section}'")
    storage = cfg["storage"]
    if storage.get("format") not in SUPPORTED_FORMATS:
        raise ConfigError(f"storage.format must be one of {sorted(SUPPORTED_FORMATS)}, got {storage.get('format')!r}")
    for key in ("base_path", "input_path", "reports_path"):
        if not storage.get(key):
            raise ConfigError(f"storage.{key} is required")
    entities = cfg["ingestion"].get("entities") or {}
    for name in ENTITY_NAMES:
        if not entities.get(name):
            raise ConfigError(f"ingestion.entities.{name} needs at least one file pattern")
    q = cfg["quality"]
    for key in ("allowed_payment_types", "allowed_customer_types", "allowed_segments", "allowed_fuel_types"):
        if not q.get(key):
            raise ConfigError(f"quality.{key} must be a non-empty list")
    if not (0 < q.get("max_discount_pct", 0) <= 100):
        raise ConfigError("quality.max_discount_pct must be in (0, 100]")
    if q.get("max_quantity_per_sale", 0) < 1:
        raise ConfigError("quality.max_quantity_per_sale must be >= 1")
    g = cfg["gold"]
    if not (0 < g["abc_a_threshold"] < g["abc_b_threshold"] <= 1):
        raise ConfigError("gold: require 0 < abc_a_threshold < abc_b_threshold <= 1")
    if g["dim_date_start"] >= g["dim_date_end"]:
        raise ConfigError("gold.dim_date_start must be before gold.dim_date_end")
    if g["fiscal_year_start_month"] not in range(1, 13):
        raise ConfigError("gold.fiscal_year_start_month must be 1-12")


def join_path(base: str, *parts: str) -> str:
    """Join paths for local folders and URI-style locations (abfss://, dbfs:/)."""
    if "://" in base or base.startswith("dbfs:/"):
        return "/".join([base.rstrip("/"), *[p.strip("/") for p in parts]])
    return str(Path(base, *parts))


def table_path(cfg: dict[str, Any], layer: str, name: str) -> str:
    """Location of a lakehouse table: <base>/<layer>/<name> (layer: bronze|silver|rejects|gold)."""
    if layer not in {"bronze", "silver", "rejects", "gold"}:
        raise ConfigError(f"Unknown layer: {layer}")
    return join_path(cfg["storage"]["base_path"], layer, name)


def state_file_path(cfg: dict[str, Any]) -> Path:
    """Where the incremental-load state lives. Must be openable by plain Python."""
    explicit = cfg["storage"].get("state_path")
    base = explicit if explicit else join_path(cfg["storage"]["base_path"], "_state")
    if "://" in str(base) or str(base).startswith("dbfs:/"):
        raise ConfigError("storage.state_path must be a path Python can open (local, /dbfs/... or /Volumes/...)")
    return Path(base) / "state.json"


def to_spark_path(path: str) -> str:
    """Translate Databricks FUSE paths (/dbfs/x) to Spark URIs (dbfs:/x)."""
    return "dbfs:/" + path[len("/dbfs/"):] if path.startswith("/dbfs/") else path
