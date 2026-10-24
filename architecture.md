# Architecture

Mermaid diagrams render natively on GitHub.

## 1. End-to-end flow

```mermaid
flowchart LR
    subgraph SRC["Landing zone (CSV extracts)"]
        A1["dealerships_*.csv"]
        A2["products_*.csv"]
        A3["sales_*.csv"]
        A4["inventory_*.csv"]
    end

    subgraph LAKE["Lakehouse: Parquet locally, Delta Lake on ADLS Gen2"]
        B[("BRONZE<br/>raw text + lineage<br/>per entity")]
        S[("SILVER<br/>typed, validated, deduped<br/>upsert on business key")]
        Q[("REJECTS<br/>quarantine + rule names")]
        subgraph STAR["GOLD star schema"]
            D1["dim_date"]
            D2["dim_dealership"]
            D3["dim_product"]
            F1["fact_sales"]
            F2["fact_inventory_snapshot"]
        end
        subgraph MARTS["GOLD KPI marts (SQL files)"]
            M1["mart_monthly_sales"]
            M2["mart_dealership_monthly"]
            M3["mart_product_kpis"]
            M4["mart_inventory_weekly"]
        end
    end

    A1 & A2 & A3 & A4 --> B --> S
    S -->|invalid rows| Q
    S --> STAR --> MARTS
    MARTS & STAR --> PBI["Power BI (design provided)"]
    MARTS & STAR --> SQL["Spark SQL / Databricks SQL"]

    ST[["state.json: file checksums + stage status"]]
    B <--> ST
    S --> DQ["Data quality report<br/>(per batch, per entity)"]
    ADF["Azure Data Factory (template)"] -. triggers .-> B
```

## 2. Incremental run: late sales, corrections and duplicates

```mermaid
sequenceDiagram
    autonumber
    participant L as Landing zone
    participant P as Pipeline
    participant St as State store
    participant S as Silver
    participant G as Gold

    L->>P: new files (reference, sales, inventory)
    P->>St: checksum already ingested?
    St-->>P: skip known files, keep new ones
    P->>S: reference data first (dealerships, products) as SCD Type 1
    P->>S: sales: validate, check dealership/product exist, quarantine bad rows
    Note over P,S: de-duplicate in batch; compare with Silver:<br/>same key + same values = duplicate, same key + new values = correction,<br/>new key older than dealership watermark = late arrival
    P->>S: upsert on sale_id (latest ingestion wins)
    P->>G: rebuild dimensions, then ONLY the fact months touched
    P->>G: recompute KPI marts (small, full recompute)
    P->>St: mark batch complete
    Note over P,G: inventory reconciliation now shows whether any sales are still missing
```

## 3. Star schema

```mermaid
erDiagram
    dim_date ||--o{ fact_sales : "date_key"
    dim_dealership ||--o{ fact_sales : "dealership_key"
    dim_product ||--o{ fact_sales : "product_key"
    dim_date ||--o{ fact_inventory_snapshot : "date_key"
    dim_dealership ||--o{ fact_inventory_snapshot : "dealership_key"
    dim_product ||--o{ fact_inventory_snapshot : "product_key"

    fact_sales {
        string sale_id PK
        int date_key FK
        long dealership_key FK
        long product_key FK
        int quantity
        double gross_amount
        double discount_amount
        double net_amount
        double cost_amount
        double gross_profit
        boolean is_late_arrival
        string year_month "partition"
    }
    fact_inventory_snapshot {
        int date_key FK
        long dealership_key FK
        long product_key FK
        int units_on_hand
        int units_received
        double stock_value_at_cost
        string year_month "partition"
    }
    dim_date {
        int date_key PK
        date date
        string year_month
        int fiscal_year
        int fiscal_quarter
        boolean is_weekend
    }
    dim_dealership {
        long dealership_key PK
        string dealership_id
        string region
        double monthly_sales_target
    }
    dim_product {
        long product_key PK
        string product_id
        string segment
        string fuel_type
        double list_price
        double unit_cost
    }
```

Grain: `fact_sales` = one sale; `fact_inventory_snapshot` = dealership x product x Sunday.

## 4. What runs where

| Component | Local (no Azure) | Azure |
|---|---|---|
| Bronze / Silver / Gold star / marts (PySpark + SQL) | Implemented and tested | Same code with `storage.format: delta` |
| Parquet storage | Implemented and tested | n/a |
| Delta Lake storage | Implemented, needs internet once for jars | Built into Databricks |
| Data quality reports | Implemented and tested | Same |
| Power BI | Design and DAX provided, no .pbix | Connect to Databricks SQL or Synapse serverless |
| ADF, Databricks notebook, Synapse views, `deploy.sh`, Azure DevOps | n/a | Templates, not deployed |
| GitHub Actions CI | Workflow provided | n/a |
