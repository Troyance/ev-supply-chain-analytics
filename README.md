# ev-supply-chain-analytics

Side project for getting hands-on with Databricks. I'm using the TPC-H sample
data that comes with every workspace and treating it like an EV maker's supply
chain — suppliers, parts, purchase orders, and shipments with promised vs.
actual dates.

The goal is to answer a few questions I'd expect a supply chain analytics team
to care about:

- Which suppliers are late the most, and is it getting worse?
- What do lead times look like?
- Which expensive parts are we only buying from one supplier?
- How much of our spend is concentrated in one country or region?

## How it's set up

Standard bronze / silver / gold layout, all in `workspace.ev_supply_chain`:

- **bronze** – raw copies of the source tables
- **silver** – cleaned up facts and dimensions
- **scenarios** – tpch is perfectly uniform, so this adds some realistic mess on top
  (late suppliers, a chip shortage, slower asia freight, etc.) — see `notebooks/02b_scenarios.py`
- **gold** – the tables that actually answer the questions above

Bronze and gold are SQL in `sql/`, silver and scenarios are PySpark notebooks in `notebooks/`.

## Running it

You'll need the Databricks CLI configured against a workspace with a SQL
warehouse. Then run the files in order.
