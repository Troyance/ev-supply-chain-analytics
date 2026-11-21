# Databricks notebook source
# MAGIC %md
# MAGIC # planning setup
# MAGIC
# MAGIC Planning parameters and demand history, on top of production (03) and the supplier scorecard (05):
# MAGIC
# MAGIC - `dim_part_planning` - main supplier, lead time and buffer class (JIT / kanban / standard / strategic) per part
# MAGIC - `fct_service_demand` - weekly service parts demand at the DC, driven by how many vehicles are on the road
# MAGIC - `fct_inventory_snapshot` - what's on hand and on order right now
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
    """deterministic 'random' int in [0, n). the salt keeps different draws independent - without it, every bucket on part_id is correlated with the bom sample filter"""
    return F.pmod(F.xxhash64(F.lit(salt), *cols), F.lit(n))

# COMMAND ----------

# MAGIC %md
# MAGIC ## inputs

# COMMAND ----------

builds = spark.table(f"{SCHEMA}.fct_vehicle_builds")
bom = spark.table(f"{SCHEMA}.dim_bom")
catalog = spark.table(f"{SCHEMA}.dim_part_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## part planning parameters
# MAGIC
# MAGIC Main supplier = the primary source from `dim_sourcing`. Lead time and its variability come from what
# MAGIC that supplier actually delivered (the scorecard), not what they quoted. Buffer class drives the replenishment logic later:
# MAGIC
# MAGIC | class | who | lead time used | service level |
# MAGIC |---|---|---|---|
# MAGIC | JIT | body / wiring from nearby (americas) suppliers | transit only | small fixed buffer |
# MAGIC | KANBAN | fasteners & fittings | transit only | min/max |
# MAGIC | STRATEGIC | battery materials, electronics | full supplier lead time | 99% |
# MAGIC | STANDARD | everything else | full supplier lead time | 95% |

# COMMAND ----------

main_supplier = (spark.table(f"{SCHEMA}.dim_sourcing").filter("sourcing_role = 'primary'")
                 .select("part_id", "supplier_id", F.col("unit_price").alias("unit_cost")))

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
    catalog.select("part_id", "part_number", "part_name", "component_category")
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
    # cheap stuff comes in big boxes, expensive modules one at a time
    .withColumn("pack_size",
                F.when(F.col("unit_cost") < 1, 1000).when(F.col("unit_cost") < 20, 100)
                 .when(F.col("unit_cost") < 200, 10).otherwise(1))
    .select("part_id", "part_number", "part_name", "component_category", "supplier_id", "supplier_name",
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
                 .select("part_id", "component_category").orderBy("part_id").toPandas())

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
# MAGIC Fake but plausible. A part that is being managed properly sits around its safety stock plus half a
# MAGIC review cycle on hand, with roughly one lead time of usage already on order (in transit). Both get
# MAGIC scattered so some parts are short, some are fine and a few are sitting on too much.

# COMMAND ----------

recent_drr = (part_demand
              .filter(F.col("week_start") > F.date_sub(F.lit(weeks[-1].date()), 56))  # last 8 full weeks
              .groupBy("site_id", "part_id")
              .agg((F.sum("units") / 56).alias("recent_drr")))

# rough safety stock in days by class, about what 08_planning ends up with
safety_days = (F.when(F.col("buffer_class") == "JIT", 0.5).when(F.col("buffer_class") == "KANBAN", 3)
                .when(F.col("buffer_class") == "STANDARD", 11).otherwise(21))

inventory = save(
    recent_drr.filter("recent_drr > 0")
    .join(part_planning.select("part_id", "buffer_class", "planning_lead_time_days", "review_period_days"), "part_id")
    .withColumn("on_hand_days", (safety_days + F.col("review_period_days") / 2)
                * (0.2 + bucket("site_id", "part_id", n=200, salt="cover") / 100))
    .withColumn("on_order_days", F.when(bucket("part_id", "site_id", n=100, salt="po") < 85,
                F.col("planning_lead_time_days") * (0.6 + bucket("part_id", "site_id", n=60, salt="po_qty") / 100))
                 .otherwise(0))
    .withColumn("on_hand_qty", F.round(F.col("recent_drr") * F.col("on_hand_days")).cast("bigint"))
    .withColumn("on_order_qty", F.round(F.col("recent_drr") * F.col("on_order_days")).cast("bigint"))
    .select(F.lit(SNAPSHOT_DATE).cast("date").alias("snapshot_date"), "site_id", "part_id",
            "on_hand_qty", "on_order_qty"),
    "fct_inventory_snapshot")

# COMMAND ----------

import json
result = {t: spark.table(f"{SCHEMA}.{t}").count() for t in
          ["dim_part_planning", "fct_service_demand", "fct_part_demand_weekly", "fct_inventory_snapshot"]}
result["buffer_classes"] = {r[0]: r[1] for r in part_planning.groupBy("buffer_class").count().collect()}
result["lead_time_by_class"] = {r[0]: [round(r[1], 1), round(r[2], 1)] for r in part_planning.groupBy("buffer_class")
                                .agg(F.avg("planning_lead_time_days"), F.avg("lead_time_stddev_days")).collect()}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
