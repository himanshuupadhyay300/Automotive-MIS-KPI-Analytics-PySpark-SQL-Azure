#!/usr/bin/env bash
# TEMPLATE - these commands have NOT been executed from this repository.
# Review names/region, run `az login`, then go step by step. Resources incur Azure charges.
set -euo pipefail

LOCATION="${LOCATION:-centralindia}"
PREFIX="${PREFIX:-automis}"
RG="rg-${PREFIX}-dev"
STORAGE="st${PREFIX}dev$RANDOM"          # globally unique, lower-case alphanumerics
KV="kv-${PREFIX}-dev-$RANDOM"
ADF="adf-${PREFIX}-dev"
DBX="dbx-${PREFIX}-dev"
SYN="syn${PREFIX}dev$RANDOM"

az extension add --name datafactory --only-show-errors
az extension add --name databricks --only-show-errors
az extension add --name synapse --only-show-errors

az group create -n "$RG" -l "$LOCATION"

# ADLS Gen2 = storage account with hierarchical namespace
az storage account create -n "$STORAGE" -g "$RG" -l "$LOCATION" --sku Standard_LRS --kind StorageV2 \
  --hns true --min-tls-version TLS1_2 --allow-blob-public-access false
for c in landing lakehouse; do az storage container create --account-name "$STORAGE" -n "$c" --auth-mode login; done

az keyvault create -n "$KV" -g "$RG" -l "$LOCATION" --enable-rbac-authorization true
az databricks workspace create -n "$DBX" -g "$RG" -l "$LOCATION" --sku premium
az datafactory create -n "$ADF" -g "$RG" -l "$LOCATION"

# Optional: Synapse workspace (serverless SQL over the Gold Delta tables for Power BI)
# az synapse workspace create -n "$SYN" -g "$RG" -l "$LOCATION" --storage-account "$STORAGE" \
#   --file-system lakehouse --sql-admin-login-user sqladmin --sql-admin-login-password "<from-key-vault>"

# Least privilege: ADF managed identity may read/write blobs in this storage account only
ADF_PRINCIPAL=$(az datafactory show -n "$ADF" -g "$RG" --query identity.principalId -o tsv)
STORAGE_ID=$(az storage account show -n "$STORAGE" -g "$RG" --query id -o tsv)
az role assignment create --assignee-object-id "$ADF_PRINCIPAL" --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" --scope "$STORAGE_ID"

echo "Next: store secrets in $KV, create a Key Vault-backed Databricks secret scope, import pipelines/adf/*.json,"
echo "      upload CSV extracts to abfss://landing@$STORAGE.dfs.core.windows.net/"
