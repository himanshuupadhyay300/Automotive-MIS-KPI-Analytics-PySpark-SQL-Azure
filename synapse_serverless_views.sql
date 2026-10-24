-- OPTIONAL / TEMPLATE (not executed): expose the Gold Delta tables through Synapse serverless SQL for Power BI.
-- Replace <storage_account>. Authenticate with the workspace managed identity (no keys in SQL).
CREATE DATABASE mis;
GO
USE mis;
GO
CREATE VIEW dbo.fact_sales AS
SELECT * FROM OPENROWSET(BULK 'https://<storage_account>.dfs.core.windows.net/lakehouse/mis/gold/fact_sales/', FORMAT = 'DELTA') AS r;
GO
CREATE VIEW dbo.dim_dealership AS
SELECT * FROM OPENROWSET(BULK 'https://<storage_account>.dfs.core.windows.net/lakehouse/mis/gold/dim_dealership/', FORMAT = 'DELTA') AS r;
GO
CREATE VIEW dbo.dim_product AS
SELECT * FROM OPENROWSET(BULK 'https://<storage_account>.dfs.core.windows.net/lakehouse/mis/gold/dim_product/', FORMAT = 'DELTA') AS r;
GO
CREATE VIEW dbo.dim_date AS
SELECT * FROM OPENROWSET(BULK 'https://<storage_account>.dfs.core.windows.net/lakehouse/mis/gold/dim_date/', FORMAT = 'DELTA') AS r;
GO
CREATE VIEW dbo.fact_inventory_snapshot AS
SELECT * FROM OPENROWSET(BULK 'https://<storage_account>.dfs.core.windows.net/lakehouse/mis/gold/fact_inventory_snapshot/', FORMAT = 'DELTA') AS r;
GO
-- Repeat for the four mart_* tables if the report should read the pre-aggregated marts.
