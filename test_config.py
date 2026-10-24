import pytest

from src.utils.config import join_path, load_config, state_file_path, table_path, to_spark_path
from src.utils.exceptions import ConfigError
from tests.conftest import CONFIG_PATH


def test_default_config_loads():
    cfg = load_config(CONFIG_PATH)
    assert cfg["storage"]["format"] == "parquet" and cfg["gold"]["fiscal_year_start_month"] == 4


def test_env_override_and_explicit_priority(monkeypatch):
    monkeypatch.setenv("MIS_BASE_PATH", "/tmp/from-env")
    assert load_config(CONFIG_PATH)["storage"]["base_path"] == "/tmp/from-env"
    assert load_config(CONFIG_PATH, {"storage": {"base_path": "/tmp/explicit"}})["storage"]["base_path"] == "/tmp/explicit"


@pytest.mark.parametrize(
    "override, message",
    [
        ({"storage": {"format": "csv"}}, "storage.format"),
        ({"gold": {"abc_a_threshold": 0.99}}, "abc_a_threshold"),
        ({"gold": {"fiscal_year_start_month": 13}}, "fiscal_year_start_month"),
        ({"quality": {"max_discount_pct": 0}}, "max_discount_pct"),
        ({"gold": {"dim_date_start": "2030-01-01"}}, "dim_date_start"),
    ],
)
def test_invalid_config_rejected(override, message):
    with pytest.raises(ConfigError, match=message):
        load_config(CONFIG_PATH, override)


def test_missing_file():
    with pytest.raises(ConfigError):
        load_config("nope.yaml")


def test_paths():
    cfg = load_config(CONFIG_PATH, {"storage": {"base_path": "abfss://lake@acct.dfs.core.windows.net/mis"}})
    assert table_path(cfg, "gold", "fact_sales") == "abfss://lake@acct.dfs.core.windows.net/mis/gold/fact_sales"
    with pytest.raises(ConfigError):
        table_path(cfg, "platinum", "x")
    with pytest.raises(ConfigError, match="state_path"):
        state_file_path(cfg)
    assert join_path("a", "b") == "a/b" and to_spark_path("/dbfs/x") == "dbfs:/x"
