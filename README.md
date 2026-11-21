# EV Supply Chain Analytics

A hands-on Databricks project modeling supplier performance and sourcing risk
for an electric-vehicle manufacturer.

It uses the TPC-H sample dataset that ships with every Databricks workspace,
reframed as a supply chain: suppliers, parts, approved supplier–part pairs,
purchase orders, and PO lines with ship, commit and receipt dates.

## Architecture — medallion layers

| Layer  | Purpose | Example tables |
|--------|---------|----------------|
| Bronze | Raw copies of source data plus ingestion metadata | `bronze_lineitem`, `bronze_supplier` |
| Silver | Cleaned, conformed facts and dimensions | `fct_shipments`, `dim_supplier`, `dim_part` |
| Gold   | Business-ready marts | supplier scorecard, single-source risk, geographic concentration |

All tables live in Unity Catalog under `workspace.ev_supply_chain`.

## Questions the gold layer answers

- Which suppliers deliver late most often, and is it getting worse?
- What are average lead times, and how much do they vary, by supplier and shipping mode?
- Which high-spend parts depend on a single supplier?
- How concentrated is spend by country and region?

## Repo layout

```
sql/
  01_bronze.sql   # raw landing tables
  02_silver.sql   # (coming) cleaned facts & dimensions
  03_gold.sql     # (coming) analytics marts
```

## Running it

Requires the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) and a SQL warehouse.

```bash
databricks configure --host https://<your-workspace>.cloud.databricks.com
databricks experimental aitools tools query -f sql/01_bronze.sql
```

## Tech

Databricks SQL · Delta Lake · Unity Catalog · serverless SQL warehouse
