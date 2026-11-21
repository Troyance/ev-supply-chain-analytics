# ev-supply-chain-analytics

Side project for getting hands-on with Databricks. It models the supply chain of a made-up EV maker:
three vehicle models built at two plants, about 1,900 parts, about 800 suppliers, and a service parts DC.

It started on the TPC-H sample data, but TPC-H is a benchmark - every supplier is late 64% of the time,
everything costs about the same and nothing has a trend. So the supplier master still comes from TPC-H,
and the rest is synthetic but calibrated to look like the real thing (about 95k vehicles a year, about
$45k of parts per vehicle, about 93% supplier on-time delivery, 3 week lead times locally and 12 weeks
from asia).

Questions it answers:

- which suppliers are late, and which ones are getting worse?
- where are lead times long, and what happened during the chip shortage?
- which parts depend on one supplier who isn't reliable?
- how many vehicles will we build in the next 13 weeks, and what do we need to buy for that?

## Pipeline

Run in this order. SQL files go through `./run.sh`, notebooks through `./run_notebook.sh` (serverless).

| step | what it does |
|---|---|
| `sql/01_bronze.sql` | raw copies of the tpch tables |
| `notebooks/02_silver.py` | cleaned up tpch facts and dimensions (pyspark) |
| `notebooks/03_production.py` | build schedule, part catalog with real names and costs, BOM |
| `notebooks/04_purchasing.py` | suppliers, sourcing, and PO delivery history generated from the build schedule |
| `sql/05_gold.sql` | supplier scorecard, single-source risk, geography, monthly trends |
| `notebooks/06_planning_setup.py` | buffer classes, lead times, service parts demand, inventory snapshot |
| `notebooks/07_forecast.py` | lightgbm forecasts for builds and service parts, logged to mlflow (needs `lightgbm mlflow` as deps) |
| `sql/08_planning.sql` | part demand plan, buy signals, forecast accuracy |
| `sql/09_docs.sql` | unity catalog table and column descriptions |
| `sql/10_metric_views.sql` | metric views - the semantic layer (run with `python3 run_api.py`, the cli strips yaml indentation) |

The dashboard is defined in `dashboards/build_dashboard.py`.

## A few things worth knowing

- plant part demand is never forecast directly. builds are forecast, then exploded through the BOM. only
  service parts get their own forecast.
- forecasts are append-only with a run id, so old forecasts can be scored against what actually happened.
- the declining-supplier flag uses the trend in days late, not a year-over-year on-time comparison. the
  yearly comparison was too noisy to be useful.
- ratios like on-time rate and WAPE live in the metric views so they get computed after aggregating,
  never averaged.
