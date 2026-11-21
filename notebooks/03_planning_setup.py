# Databricks notebook source
# MAGIC %md
# MAGIC # planning setup
# MAGIC
# MAGIC Builds the planning side of the world on top of the supplier data:
# MAGIC
# MAGIC - `dim_site` – two plants and a service parts DC
# MAGIC - `dim_vehicle_model` – three vehicle models
# MAGIC - `fct_vehicle_builds` – weekly builds per plant/model since 2020 (ramps, launches, shutdowns, a chip shortage)
# MAGIC - `dim_bom` – which parts go into which vehicle, and how many
# MAGIC - `dim_part_planning` – main supplier, lead time and buffer class (JIT / kanban / standard / strategic) per part
# MAGIC - `fct_service_demand` – weekly service parts demand at the DC, driven by how many vehicles are on the road
# MAGIC - `fct_inventory_snapshot` – what's on hand and on order right now
# MAGIC
# MAGIC Plant part demand is *dependent* demand: builds × BOM. Only the service DC has true independent demand.

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

SCHEMA = "workspace.ev_supply_chain"
SNAPSHOT_DATE = "2026-11-21"
rng = np.random.default_rng(42)

# weekly buckets, monday start. history runs through the last full week before the snapshot
weeks = pd.date_range("2020-01-06", "2026-11-09", freq="W-MON")

def save(pdf_or_sdf, name):
    sdf = spark.createDataFrame(pdf_or_sdf) if isinstance(pdf_or_sdf, pd.DataFrame) else pdf_or_sdf
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.{name}")
    return spark.table(f"{SCHEMA}.{name}")

def bucket(*cols, n, salt):
    """deterministic 'random' int in [0, n). the salt keeps different draws independent —
    without it, every bucket on part_id is correlated with the bom sample filter"""
    return F.pmod(F.xxhash64(F.lit(salt), *cols), F.lit(n))

# COMMAND ----------

# MAGIC %md
# MAGIC ## sites and models

# COMMAND ----------

sites = save(pd.DataFrame(
    [("PLT1", "Plant 1 - Midwest", "plant"),
     ("PLT2", "Plant 2 - Southeast", "plant"),
     ("SDC1", "Service Parts DC", "service_dc")],
    columns=["site_id", "site_name", "site_type"]), "dim_site")

models = save(pd.DataFrame(
    [("PU", "Pickup"), ("SUV", "SUV"), ("VAN", "Delivery Van")],
    columns=["model_id", "model_name"]), "dim_vehicle_model")

# COMMAND ----------

# MAGIC %md
# MAGIC ## vehicle builds
# MAGIC
# MAGIC Each production line launches, ramps up on an S-curve, then grows slowly. On top of that:
# MAGIC summer changeover shutdown, holiday shutdown, end-of-quarter push, and a chip shortage in spring 2022.
# MAGIC Plant 2 only launched in March 2026 so it's still mid-ramp — that's the hard one to forecast.

# COMMAND ----------

# (site, model, launch week, peak weekly units, weeks to ramp)
lines = [
    ("PLT1", "PU",  "2021-09-13", 600, 70),
    ("PLT1", "SUV", "2021-12-06", 700, 70),
    ("PLT1", "VAN", "2022-03-07", 400, 60),
    ("PLT2", "SUV", "2026-03-02", 500, 52),
]

def seasonality(week):
    woy = week.isocalendar()[1]
    m = 1.0
    if woy in (27, 28):
        m *= 0.3                                  # summer model-year changeover
    if woy >= 52 or woy == 1:
        m *= 0.5                                  # holiday shutdown
    if week.month in (3, 6, 9, 12) and week.day >= 16:
        m *= 1.12                                 # end of quarter push
    if pd.Timestamp("2022-03-01") <= week <= pd.Timestamp("2022-06-30"):
        m *= 0.75                                 # chip shortage
    return m

rows = []
for site, model, launch, peak, ramp in lines:
    launch = pd.Timestamp(launch)
    for w in weeks[weeks >= launch]:
        age = (w - launch).days / 7
        s_curve = 1 / (1 + np.exp(-(age - ramp / 2) / (ramp / 8)))
        growth = 1 + 0.08 * max(0, (w - pd.Timestamp("2024-01-01")).days / 365)
        units = peak * s_curve * growth * seasonality(w) * rng.lognormal(0, 0.07)
        rows.append((w.date(), site, model, int(round(units))))

builds = save(pd.DataFrame(rows, columns=["week_start", "site_id", "model_id", "units"]), "fct_vehicle_builds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bom
# MAGIC
# MAGIC A sample of ~2k parts from the catalog, each used in some of the models. Quantity per vehicle depends on
# MAGIC the category (lots of fasteners, a few big modules).

# COMMAND ----------

qty = (F.when(F.col("component_category") == "Fasteners & fittings", 20 + bucket("part_id", n=40, salt="qty"))
        .when(F.col("component_category") == "Battery materials",    4 + bucket("part_id", n=8, salt="qty"))
        .when(F.col("component_category") == "Wiring & harnesses",   2 + bucket("part_id", n=6, salt="qty"))
        .when(F.col("component_category") == "Electronics",          1 + bucket("part_id", n=4, salt="qty"))
        .otherwise(1 + bucket("part_id", n=5, salt="qty")))

bom = save(
    spark.table(f"{SCHEMA}.dim_part")
    .filter(bucket("part_id", n=500, salt="bom") == 0)
    .select("part_id", "component_category")
    .crossJoin(models.select("model_id"))
    .filter(bucket("part_id", "model_id", n=3, salt="bom_model") != 0)   # each part is in ~2 of the 3 models
    .withColumn("qty_per_vehicle", qty.cast("int"))
    .select("model_id", "part_id", "qty_per_vehicle"),
    "dim_bom")

# COMMAND ----------

# MAGIC %md
# MAGIC ## part planning parameters
# MAGIC
# MAGIC Main supplier = cheapest approved supplier (same rule the scenarios notebook used for battery parts).
# MAGIC Buffer class drives the replenishment logic later:
# MAGIC
# MAGIC | class | who | lead time used | service level |
# MAGIC |---|---|---|---|
# MAGIC | JIT | body / wiring from nearby (americas) suppliers | transit only | small fixed buffer |
# MAGIC | KANBAN | fasteners & fittings | transit only | min/max |
# MAGIC | STRATEGIC | battery materials, electronics | full supplier lead time | 99% |
# MAGIC | STANDARD | everything else | full supplier lead time | 95% |

# COMMAND ----------

main_supplier = (spark.table(f"{SCHEMA}.dim_part_supplier")
                 .groupBy("part_id")
                 .agg(F.min_by("supplier_id", "unit_cost").alias("supplier_id"),
                      F.min("unit_cost").alias("unit_cost")))

scorecard = spark.table(f"{SCHEMA}.gold_supplier_scorecard").select(
    "supplier_id", "supplier_name", "region", "avg_lead_time_days", "lead_time_stddev", "tier")

cat, region = F.col("component_category"), F.col("region")
buffer_class = (F.when(cat == "Fasteners & fittings", "KANBAN")
                 .when(cat.isin("Body & chassis", "Wiring & harnesses") & (region == "America"), "JIT")
                 .when(cat.isin("Battery materials", "Electronics"), "STRATEGIC")
                 .otherwise("STANDARD"))

transit_days = (F.when(region == "America", 3).when(region == "Europe", 21)
                 .when(region == "Asia", 35).otherwise(28))

part_planning = (
    bom.select("part_id").distinct()
    .join(spark.table(f"{SCHEMA}.dim_part").select("part_id", "part_name", "component_category"), "part_id")
    .join(main_supplier, "part_id")
    .join(scorecard, "supplier_id")
    .withColumn("buffer_class", buffer_class)
    .withColumn("transit_days", transit_days)
    .withColumn("planning_lead_time_days",
                F.when(F.col("buffer_class").isin("JIT", "KANBAN"), F.col("transit_days"))
                 .otherwise(F.round("avg_lead_time_days").cast("int")))
    .withColumn("lead_time_stddev_days",
                F.when(F.col("buffer_class").isin("JIT", "KANBAN"), F.lit(1.0))
                 .otherwise(F.col("lead_time_stddev")))
    .withColumn("review_period_days",
                F.when(F.col("buffer_class") == "JIT", 1)
                 .when(F.col("buffer_class") == "STRATEGIC", 14)
                 .otherwise(7))
    .withColumn("service_level_z",
                F.when(F.col("buffer_class") == "STRATEGIC", 2.33).otherwise(1.65))
    .withColumn("pack_size",
                F.when(cat == "Fasteners & fittings", 500).when(cat == "Battery materials", 100).otherwise(10))
    .select("part_id", "part_name", "component_category", "supplier_id", "supplier_name",
            F.col("region").alias("supplier_region"), F.col("tier").alias("supplier_tier"),
            "unit_cost", "buffer_class", "planning_lead_time_days", "lead_time_stddev_days",
            "review_period_days", "service_level_z", "pack_size")
)
part_planning = save(part_planning, "dim_part_planning")

# COMMAND ----------

# MAGIC %md
# MAGIC ## service parts demand
# MAGIC
# MAGIC Vehicles in the field break things. Weekly demand at the DC for a part ≈ fleet size × that part's failure
# MAGIC rate, drawn from a Poisson so small parts are lumpy and full of zero weeks. Body parts spike in winter
# MAGIC (collisions). Roughly a quarter of the BOM parts are stocked as service parts.

# COMMAND ----------

fleet = (builds.toPandas().groupby("week_start")["units"].sum()
         .reindex([w.date() for w in weeks], fill_value=0).cumsum())

service_parts = (part_planning
                 .filter(F.col("component_category").isin("Body & chassis", "Electronics", "Wiring & harnesses"))
                 .filter(bucket("part_id", n=4, salt="service") == 0)
                 .select("part_id", "component_category").toPandas())

base_rate = {"Body & chassis": 0.08, "Electronics": 0.05, "Wiring & harnesses": 0.03}  # per 1k vehicles per week
rows = []
for part_id, cat_name in service_parts.itertuples(index=False):
    rate = base_rate[cat_name] * rng.lognormal(0, 0.9)
    for w, f in fleet.items():
        if f == 0:
            continue
        season = 1.3 if (cat_name == "Body & chassis" and w.month in (12, 1, 2)) else 1.0
        rows.append((w, "SDC1", int(part_id), int(rng.poisson(f / 1000 * rate * season))))

service = save(pd.DataFrame(rows, columns=["week_start", "site_id", "part_id", "units"]), "fct_service_demand")

# COMMAND ----------

# MAGIC %md
# MAGIC ## actual demand (one table for both kinds)
# MAGIC
# MAGIC Plants: builds × BOM. DC: service demand. This is what forecasts get scored against and what the
# MAGIC historical run rate comes from.

# COMMAND ----------

plant_usage = (builds.join(bom, "model_id")
               .groupBy("week_start", "site_id", "part_id")
               .agg(F.sum(F.col("units") * F.col("qty_per_vehicle")).alias("units")))

part_demand = save(plant_usage.unionByName(service), "fct_part_demand_weekly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## inventory snapshot
# MAGIC
# MAGIC Fake but plausible: on-hand is some number of days of recent usage, scattered around what you'd expect for
# MAGIC the buffer class — so some parts are fine, some are short, some are sitting on way too much.

# COMMAND ----------

recent_drr = (part_demand
              .filter(F.col("week_start") > F.date_sub(F.lit(weeks[-1].date()), 56))  # last 8 full weeks
              .groupBy("site_id", "part_id")
              .agg((F.sum("units") / 56).alias("recent_drr")))

typical_days = (F.when(F.col("buffer_class") == "JIT", 2).when(F.col("buffer_class") == "KANBAN", 10)
                 .when(F.col("buffer_class") == "STANDARD", 45).otherwise(100))

inventory = save(
    recent_drr.filter("recent_drr > 0")
    .join(part_planning.select("part_id", "buffer_class", "pack_size"), "part_id")
    .withColumn("days_cover", typical_days * (0.2 + bucket("site_id", "part_id", n=200, salt="cover") / 100))
    .withColumn("on_hand_qty", F.round(F.col("recent_drr") * F.col("days_cover")).cast("bigint"))
    .withColumn("on_order_qty",
                F.when(bucket("part_id", "site_id", n=3, salt="po") == 0,
                       F.round(F.col("recent_drr") * typical_days * 0.5).cast("bigint")).otherwise(0))
    .select(F.lit(SNAPSHOT_DATE).cast("date").alias("snapshot_date"), "site_id", "part_id",
            "on_hand_qty", "on_order_qty"),
    "fct_inventory_snapshot")

# COMMAND ----------

import json
result = {t: spark.table(f"{SCHEMA}.{t}").count() for t in
          ["dim_site", "dim_vehicle_model", "fct_vehicle_builds", "dim_bom", "dim_part_planning",
           "fct_service_demand", "fct_part_demand_weekly", "fct_inventory_snapshot"]}
result["buffer_classes"] = {r[0]: r[1] for r in part_planning.groupBy("buffer_class").count().collect()}
result["latest_weekly_builds"] = {f"{r.site_id}-{r.model_id}": r.units for r in
                                  builds.filter(F.col("week_start") == weeks[-1].date()).collect()}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
