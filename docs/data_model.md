# data model

Kimball-style: conformed dimensions shared by every fact, one clear grain per fact table, business-facing marts on
top. dbt owns everything after bronze.

## layers

Medallion layers, one schema each in the `workspace` catalog:

| layer | schema | what's in it | built by |
|---|---|---|---|
| bronze | `ev_bronze` | source system extracts (erp, mes, wms, service) plus ml and simulation outputs | spark notebooks - in a real setup this is what fivetran lands |
| staging | `ev_staging` | `stg_` views, one per source table - renames and types only, no business logic | dbt |
| silver | `ev_silver` | `int_` (reusable business logic), `dim_`, `bridge_`, `fct_` - conformed and tested | dbt |
| gold | `ev_gold` | `mart_` tables and metric views - what dashboards and genie read | dbt |

Registered forecasting models live in `ev_ml`.

## bus matrix

Rows are business processes (facts), columns are conformed dimensions. An x means the fact joins to that dimension.

| fact | grain | type | date | site | vehicle model | part | supplier | forecast series | forecast run | policy | program |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `fct_vehicle_builds` | week x site x vehicle model | periodic snapshot | x | x | x | | | | | | |
| `fct_po_deliveries` | one delivery line | transaction | x | x | | x | x | | | | |
| `fct_part_demand` | week x site x part | periodic snapshot | x | x | | x | | | | | |
| `fct_inventory_snapshot` | snapshot date x site x part | periodic snapshot | x | x | | x | | | | | |
| `fct_forecast` | forecast run x series x origin week x target week | transaction (append-only) | x | x | x | x | | x | x | | |
| `fct_policy_simulation` | policy x site x part x day | periodic snapshot | x | x | | x | | | | x | |
| `fct_engineering_change` | one engineering change (ECO) | accumulating snapshot | x | | | x (via bridge) | | | | | x |
| `fct_issue` | one issue | transaction | x | | | x | | | | | x |
| `fct_recall_campaign` | one nhtsa recall campaign (real data) | transaction | x | | | | | | | | |
| `fct_recall_vehicle_line` | campaign x make x model x year x component (real data) | transaction | | | | | | | | | |

`bridge_part_supplier` sits between `dim_part` and `dim_supplier` because sourcing is many to many - about 13% of
parts are dual-sourced, with a volume share on each side.

The product development side has its own bridges. `bridge_bom_structure` is the multi-level engineering BOM
(vehicle > system > assembly > part), every line effective-dated, and `int_bom_explosion` walks it recursively for
any date. `bridge_eco_item` links each engineering change to the parts it touches. A test checks that today's
engineering BOM explodes to exactly the flat manufacturing BOM in `bridge_vehicle_model_part` (an EBOM / MBOM sync check).

`dim_supplier_tier_history` is a type 2 dimension (valid_from / valid_to) so a 2022 delivery is judged by the
tier the supplier had in 2022, not today's.

The two recall facts are real public data. Units affected are a campaign-level number NHTSA repeats on every vehicle
line, so they live on `fct_recall_campaign`; the line table carries none, so summing can't double count.

`dim_part` holds current parts and parts superseded by engineering changes (`lifecycle_status`), so purchase and
demand history from before a change still points at the part that was really used.

## dimensions

| dimension | grain | notes |
|---|---|---|
| `dim_date` | one row per day | iso week, week start (monday), month, quarter, shutdown weeks, chip shortage severity |
| `dim_site` | one row per site | plants and the service dc |
| `dim_vehicle_model` | one row per vehicle model | |
| `dim_part` | one row per part | the ~1,900 parts we build with. replaces both the tpch part table and the old catalog |
| `dim_supplier` | one row per active supplier | quoted lead time, region, current tier |
| `dim_supplier_tier_history` | one row per supplier per tier period | scd type 2, judged on the 12 months before each month so it never peeks |
| `bridge_part_supplier` | one row per part x supplier | primary / secondary source, volume share, unit price |
| `dim_forecast_series` | one row per thing we forecast | single `series_key` instead of nullable model_id / part_id |
| `dim_forecast_run` | one row per forecast run | backtest or production, model version, mlflow run |
| `dim_policy` | one row per ordering policy | |
| `dim_program` | one row per vehicle program | a model launching at a plant, with its start of production |
| `dim_prototype_build` | one row per prototype build | EP, VP or PPV, planned vs actual start, vehicles built |
| `dim_item` | one row per plm item | vehicles, systems, assemblies, current and superseded parts |
| `bridge_bom_structure` | one row per bom line version | multi-level engineering bom, effective-dated |
| `bridge_eco_item` | one row per change x part | what each engineering change touches |

## conventions

- surrogate keys are `<entity>_key`, built with `dbt_utils.generate_surrogate_key`. natural ids stay as `<entity>_id`
- money is `decimal(18,2)`, unit costs `decimal(18,4)`, quantities `decimal(18,2)` or `bigint`
- dates are `date`, week buckets always start monday
- every fact has a grain test (`unique_combination_of_columns`) and relationship tests to its dimensions
- primary and foreign keys are declared as unity catalog informational constraints through model contracts
- ratios (on-time rate, fill rate, wape) are never stored at row level - they live in metric views as sum / sum

## what the generators know that analytics doesn't

The purchasing generator decides each supplier's true late probability and whether it is secretly slipping. Those
columns exist in the raw table but `stg_erp__suppliers` drops them, the same way a real erp would never have them.
Everything downstream has to find the bad suppliers from delivery history alone - which is the point.
