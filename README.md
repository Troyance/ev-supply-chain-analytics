# ev-supply-chain-analytics

[![ci](https://github.com/Troyance/ev-supply-chain-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/Troyance/ev-supply-chain-analytics/actions/workflows/ci.yml)

Side project for getting hands-on with Databricks, dbt and supply chain planning. It models a made-up EV maker:
three vehicle models built at two plants, about 1,900 parts, about 800 suppliers and a service parts DC - from
purchase orders all the way to forecasts, buy signals and a comparison of ordering policies.

The data is synthetic but calibrated to look like the real thing: about 95k vehicles a year, about $45k of parts
per vehicle, about 93% supplier on-time delivery, 3 week lead times locally and 12 weeks from asia, a chip shortage
in 2021-22 and a second plant that's still ramping up.

## How it fits together

```mermaid
flowchart LR
    nhtsa[nhtsa recall file] -->|landing script| vol[(landing volume)]
    subgraph bronze [ev_bronze - spark]
        tpch[tpch supplier master]
        gen[production, purchasing,<br/>plm, planning generators]
        al[auto loader]
        ml[lightgbm forecasts]
        sim[sawtooth + policy<br/>simulations]
    end
    vol --> al --> stg
    subgraph dbt [dbt]
        stg[ev_staging<br/>stg_ views]
        silver[ev_silver<br/>dims, bridges, facts]
        gold[ev_gold<br/>marts + metric views]
    end
    tpch --> gen --> stg
    ml --> stg
    sim --> stg
    stg --> silver --> gold
    gold --> sim
    gold --> dash[ai/bi dashboard]
    gold --> genie[genie agent]
```

Spark notebooks stand in for the source systems (erp, mes, plm, wms) and the ml / simulation jobs, and land
everything in bronze. One real source (nhtsa recalls) arrives as files and is ingested by auto loader. dbt owns
everything after bronze. The simulations read gold and write back to bronze, so dbt runs twice.

## Product development

A PLM source (product lifecycle management) adds the side of the business before volume production: vehicle
programs, prototype builds (EP, VP, PPV), a multi-level engineering BOM with history, issues found in builds, and
engineering changes (ECOs) that fix them and update the BOM.

- **ecos are an accumulating snapshot** - one row per change, a date per milestone, so cycle time per stage is a column
- **the bom explodes recursively** for any date, so "what was in the vehicle at build VP1" is a query
- **engineering and manufacturing boms are reconciled by a test** - today's engineering bom must explode to exactly the
  flat bom purchasing plans with. that test caught an id collision the first time
- **history follows the bom that was valid at the time** - before a change cut in a new part, the plant consumed and
  bought the old one. 16% of historical demand ($2.8B) belongs to parts that have since been superseded, which a
  today's-bom view would have booked against their replacements
- **supplier quality sits next to delivery** - supplier-caused issues per million units on the scorecard. tier c
  (late) suppliers average 1.6x the quality issues of tier a
- **pending changes flag supply risk** - parts a pending change will replace or remove show up next to their buy
  signal and stock, so nobody reorders a part that's about to be superseded

## Real data: NHTSA recalls

Alongside the simulated company there's one real public source: every US vehicle recall since 2010 from NHTSA's bulk
file. `scripts/land_nhtsa_recalls.sh` drops each snapshot into a Unity Catalog volume (serverless here has no internet,
so the script plays the part of fivetran), and auto loader ingests new files incrementally. Real data brought real
problems: the file is a full snapshot every time (deduped in staging), pattern-matching "EV" caught RVs and cement
mixers (fixed with a curated seed of EV models), and units affected repeat on every vehicle line (so they live on a
campaign-grain fact). Since 2022, software and electronics drive more EV recalls than the battery, and about one in
five is fixed over the air.

## Data model

Kimball style - conformed dimensions, one grain per fact, marts on top. Full detail in [docs/data_model.md](docs/data_model.md).

| fact | grain | date | site | vehicle model | part | supplier | forecast | policy |
|---|---|---|---|---|---|---|---|---|
| `fct_vehicle_builds` | week x site x vehicle model | x | x | x | | | | |
| `fct_po_deliveries` | one delivery line | x | x | | x | x | | |
| `fct_part_demand` | week x site x part | x | x | | x | | | |
| `fct_inventory_snapshot` | snapshot date x site x part | x | x | | x | | | |
| `fct_forecast` | run x series x origin week x target week | x | x | x | x | | x | |
| `fct_policy_simulation` | policy x site x part x day | x | x | | x | | | x |
| `fct_engineering_change` | one engineering change | x | | | x | | | |
| `fct_issue` | one issue from a prototype build or production | x | | | x | | | |
| `fct_recall_campaign` | one real nhtsa recall campaign | x | | | | | | |

A few things that took some thought:

- **supplier tier is scd type 2.** a 2022 delivery is judged by the tier the supplier had in 2022, and each month's
  tier only uses the 12 months before it
- **parts and suppliers are many to many** (some parts are dual-sourced), and so are vehicle models and parts (the
  bom), so both are bridge tables
- **forecasts are append-only.** every run keeps its own id, so old forecasts can be scored against what actually
  happened. the grain test caught that I'd left origin week out of the grain the first time
- **plant demand is never forecast directly.** builds are forecast, then exploded through the bom. only service
  parts get their own forecast
- **the generators know things analytics doesn't** - each supplier's true late rate, which ones are secretly
  slipping. staging drops those columns, so everything downstream has to find the bad suppliers from history alone

## Testing

- every dimension, bridge, fact and mart has an enforced contract (column types), named primary and foreign keys in
  unity catalog, a grain test and relationship tests. about 245 tests run on every build
- reconciliation tests: the engineering bom must explode to exactly the manufacturing bom, and deliveries must match
  demand per part over its history
- a unit test on the supplier tier logic
- tests on the real nhtsa data warn instead of fail, so one odd public record can't block the pipeline
- ci builds everything into separate `ci_` schemas on every pull request

## Semantic layer

Metric views in `ev_gold` define every kpi once (on-time rate, wape, stockout rate, days of cover, spend at risk...).
Ratios are always sum / sum, never averaged. The dashboard and the genie agent both read these, so they always agree.

## What it shows

- **suppliers** - asia is about 4x slower than north america, electronics fell apart during the chip shortage, and
  the declining flag picks out all 11 secretly slipping suppliers with no false alarms
- **sourcing risk** - battery materials carry most of the spend that depends on unreliable suppliers, and late
  suppliers are also lower quality (tier c averages 1.6x the supplier-caused issues of tier a)
- **forecast** - lightgbm (m5 style: one global model, direct multi-horizon, tweedie for lumpy service parts) gets
  build forecasts to 7.5% wape vs 18.3% for the naive baseline
- **ordering policies** - six policies replayed on a year of real demand. at a steady plant, carrying more safety
  stock pays for itself (high service costs $197M a year against $394M for the current rules). at the ramping
  plant every policy gets the same launch pre-build, but buffers sized from trailing usage fall further behind as
  volume climbs - shortages grow from 1% of demand to 33% by month nine. only planning off the build schedule (mrp)
  keeps up, at 99.8% fill

## Running it

Everything runs as one Databricks job, defined in `databricks.yml`. The nhtsa file is landed first, from anywhere
with internet access (serverless compute here has none):

```bash
./scripts/land_nhtsa_recalls.sh     # drops the latest recall snapshot into the landing volume
databricks bundle deploy
databricks bundle run ev_supply_chain
```

For local dbt work: `./dbt.sh build` (reads your token from `~/.databrickscfg`). The dashboard is generated by
`dashboards/build_dashboard.py` and the genie agent by `genie/build_genie.py`.

## Stack

Databricks (serverless, unity catalog, volumes, auto loader, delta, metric views, ai/bi dashboards, genie, mlflow,
asset bundles) · dbt core with dbt-databricks · pyspark · lightgbm · github actions
