# Databricks notebook source
# MAGIC %md
# MAGIC # Automotive MIS & KPI Analytics - run the pipeline on Databricks
# MAGIC **Status: template, not executed against a real workspace from this repository.**
# MAGIC
# MAGIC Prerequisites: repo cloned into Databricks Repos; ADLS Gen2 `lakehouse` container; a Unity Catalog Volume for
# MAGIC landing files, state and reports (so plain Python and Spark can both reach them); secrets in a Key Vault-backed
# MAGIC scope. Never paste keys into a notebook.

# COMMAND ----------

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container", "lakehouse")
dbutils.widgets.text("landing_path", "/Volumes/main/mis/landing")
dbutils.widgets.text("state_path", "/Volumes/main/mis/state")
dbutils.widgets.text("reports_path", "/Volumes/main/mis/reports")
dbutils.widgets.dropdown("stage", "all", ["all", "bronze", "silver", "gold"])

# COMMAND ----------

import os
import sys

repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, repo_root)

from src.pipeline import run_pipeline
from src.utils.config import load_config
from src.utils.logging_utils import configure_logging

# COMMAND ----------

# Storage auth example (service principal in a secret scope) - adapt, then uncomment:
# acct = dbutils.widgets.get("storage_account")
# spark.conf.set(f"fs.azure.account.auth.type.{acct}.dfs.core.windows.net", "OAuth")
# spark.conf.set(f"fs.azure.account.oauth.provider.type.{acct}.dfs.core.windows.net",
#                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set(f"fs.azure.account.oauth2.client.id.{acct}.dfs.core.windows.net", dbutils.secrets.get("kv-mis", "sp-client-id"))
# spark.conf.set(f"fs.azure.account.oauth2.client.secret.{acct}.dfs.core.windows.net", dbutils.secrets.get("kv-mis", "sp-client-secret"))
# spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{acct}.dfs.core.windows.net",
#                f"https://login.microsoftonline.com/{dbutils.secrets.get('kv-mis', 'tenant-id')}/oauth2/token")

# COMMAND ----------

acct, container = dbutils.widgets.get("storage_account"), dbutils.widgets.get("container")
cfg = load_config(
    os.path.join(repo_root, "config", "pipeline_config.yaml"),
    {
        "app": {"environment": "databricks"},
        "storage": {
            "format": "delta",
            "base_path": f"abfss://{container}@{acct}.dfs.core.windows.net/mis",
            "input_path": dbutils.widgets.get("landing_path"),
            "state_path": dbutils.widgets.get("state_path"),
            "reports_path": dbutils.widgets.get("reports_path"),
        },
        "marts": {"sql_dir": os.path.join(repo_root, "sql", "marts")},
    },
)
configure_logging("INFO")
summary = run_pipeline(cfg, stage=dbutils.widgets.get("stage"), spark=spark)
print(summary["status"], summary["table_row_counts"])
