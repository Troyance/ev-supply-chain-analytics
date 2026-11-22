# Databricks notebook source
# MAGIC %md
# MAGIC # po deliveries
# MAGIC
# MAGIC PO history, generated the way it comes about: from the build schedule and the BOM **that was valid that week**.
# MAGIC Before an engineering change cut in a new part, the plant consumed (and bought) the old one, so superseded
# MAGIC parts show up in history until their replacement's effective date. A superseded part was bought from its
# MAGIC successor's supplier at the same price - the generator has no other sourcing for it.
# MAGIC
# MAGIC Every week, each BOM line needs (builds x qty per vehicle) units at each plant, and the part's supplier
# MAGIC delivers against that. Local body and wiring suppliers deliver daily, everyone else weekly. Each line was ordered
# MAGIC one quoted lead time before it was promised, and arrives on time or a few days early - unless the supplier is late.
# MAGIC
# MAGIC Output to `ev_bronze`: `erp_po_deliveries`.

# COMMAND ----------

from pyspark.sql import functions as F, Window

BRONZE = "workspace.ev_bronze"      # bronze: source-system extracts, written by these notebooks
SNAPSHOT_DATE = "2026-11-21"

def bucket(*cols, n, salt):
    return F.pmod(F.xxhash64(F.lit(salt), *cols), F.lit(n))

def uniform(*cols, salt):
    """deterministic uniform in (0, 1)"""
    return (bucket(*cols, n=1_000_000, salt=salt) + 0.5) / 1_000_000

def save(df, name):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{BRONZE}.{name}")
    return spark.table(f"{BRONZE}.{name}")

region = F.col("region")

# same shortage timeline as the build schedule in 03_production
def shortage_severity(d):
    start, peak, end = F.lit("2021-06-01").cast("date"), F.lit("2022-04-01").cast("date"), F.lit("2022-12-31").cast("date")
    return (F.when((d <= start) | (d >= end), 0.0)
             .when(d <= peak, F.datediff(d, start) / F.datediff(peak, start))
             .otherwise(F.datediff(end, d) / F.datediff(end, peak)))

active = spark.table(f"{BRONZE}.erp_suppliers")
sourcing = spark.table(f"{BRONZE}.erp_sourcing")
catalog = spark.table(f"{BRONZE}.erp_parts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## effective-dated BOM, leaf level
# MAGIC
# MAGIC Assembly > part lines from plm, each with its vehicle model (from the assembly) and its validity window.
# MAGIC Superseded parts are bought through their successor's sourcing.

# COMMAND ----------

items = spark.table(f"{BRONZE}.plm_items")
assemblies = (items.filter("item_type = 'assembly'")
              .select(F.col("item_id").alias("parent_item_id"), F.split("item_number", "-").getItem(2).alias("bom_model_id")))
leaf_lines = (spark.table(f"{BRONZE}.plm_bom_lines")
              .join(assemblies, "parent_item_id")
              .join(items.select(F.col("item_id").alias("child_item_id"), "superseded_by_item_id"), "child_item_id")
              .select("bom_model_id", F.col("child_item_id").alias("part_id"),
                      F.coalesce("superseded_by_item_id", "child_item_id").alias("sourcing_part_id"), "qty",
                      "effective_from", F.coalesce("effective_to", F.lit("9999-12-31").cast("date")).alias("effective_to")))

# COMMAND ----------

requirements = (spark.table(f"{BRONZE}.mes_vehicle_builds")
                .join(leaf_lines, (F.col("model_id") == F.col("bom_model_id"))
                      & F.col("week_start").between(F.col("effective_from"), F.col("effective_to")))
                .groupBy("site_id", "part_id", "sourcing_part_id", "week_start")
                .agg(F.sum(F.col("units") * F.col("qty")).alias("weekly_qty")))

lines = (requirements
         .join(sourcing.withColumnRenamed("part_id", "sourcing_part_id"), "sourcing_part_id")
         .join(catalog.select(F.col("part_id").alias("sourcing_part_id"), "component_category"), "sourcing_part_id")
         .join(active.select("supplier_id", "region", "quoted_lead_time_days", "transit_days",
                             "p_late", "late_mean_days", "is_slipping"), "supplier_id")
         .withColumn("deliveries_per_week",
                     F.when((region == "America") & F.col("component_category").isin("Body & chassis", "Wiring & harnesses"), 5)
                      .otherwise(1))
         .withColumn("day_offset", F.explode(F.sequence(F.lit(0), F.col("deliveries_per_week") - 1)))
         .withColumn("quantity", F.round(F.col("weekly_qty") * F.col("volume_share") / F.col("deliveries_per_week")).cast("int"))
         .filter("quantity > 0")
         .withColumn("promised_date", F.date_add("week_start", F.col("day_offset")))
         .withColumn("line_key", F.xxhash64("site_id", "part_id", "supplier_id", "promised_date")))

is_elec = F.col("component_category") == "Electronics"
severity = F.when(is_elec, shortage_severity(F.col("promised_date"))).otherwise(0.0)
months_slipping = F.when(F.col("is_slipping"),
                         F.greatest(F.lit(0.0), F.months_between("promised_date", F.lit("2024-01-01")))).otherwise(0.0)

lines = (lines
         .withColumn("severity", severity)
         # chip shortage: quoted lead times for electronics stretch by up to 8 weeks
         .withColumn("lead_quoted", F.col("quoted_lead_time_days") + F.round(56 * F.col("severity")).cast("int"))
         .withColumn("order_date", F.date_sub("promised_date", F.col("lead_quoted")))
         .withColumn("p_late_eff", F.least(F.lit(0.9),
                     F.col("p_late") + 0.35 * F.col("severity") + 0.02 * months_slipping))
         .withColumn("is_late", uniform("line_key", salt="late") < F.col("p_late_eff"))
         .withColumn("days_late",
                     F.when(F.col("is_late"),
                            F.ceil(-F.log(uniform("line_key", salt="mag")) * F.col("late_mean_days")
                                   * (1 + 1.5 * F.col("severity")) + 0.4 * months_slipping))
                      .otherwise(-F.floor(uniform("line_key", salt="early") * 3)))  # 0-2 days early
         .withColumn("received_date", F.date_add("promised_date", F.col("days_late").cast("int")))
         .withColumn("ship_date", F.date_sub("received_date", F.col("transit_days")))
         .filter(F.col("received_date") <= F.lit(SNAPSHOT_DATE).cast("date")))   # open lines aren't history yet

po = save(
    lines.select(
        F.col("line_key").alias("po_line_id"),
        "site_id", "part_id", "supplier_id", "sourcing_role", "quantity",
        (F.col("quantity") * F.col("unit_price")).cast("decimal(14,2)").alias("net_amount"),
        "order_date", "ship_date", "promised_date", "received_date",
        F.when(region == "America", F.when(bucket("line_key", n=5, salt="mode") == 0, "Rail").otherwise("Truck"))
         .otherwise("Ship").alias("ship_mode"),
        F.datediff("received_date", "order_date").alias("lead_time_days"),
        F.col("days_late").cast("int").alias("days_late"),
        (F.col("days_late") <= 0).alias("is_on_time")),
    "erp_po_deliveries")

# COMMAND ----------

import json

yearly = (po.groupBy(F.year("received_date").alias("y"))
          .agg(F.round(F.sum("net_amount") / 1e9, 2).alias("spend_bn"), F.count("*").alias("lines"))
          .orderBy("y").collect())
by_region = (po.join(active.select("supplier_id", "region"), "supplier_id").groupBy("region")
             .agg(F.round(F.avg("lead_time_days"), 1).alias("lead"),
                  F.round(F.avg(F.col("is_on_time").cast("int")), 3).alias("otd"),
                  F.countDistinct("supplier_id").alias("suppliers")).collect())
elec = (po.join(catalog, "part_id").filter("component_category = 'Electronics'")
        .groupBy(F.year("order_date").alias("y")).agg(F.round(F.avg("lead_time_days"), 1).alias("lead"),
                                                       F.round(F.avg(F.col("is_on_time").cast("int")), 3).alias("otd"))
        .orderBy("y").collect())

result = {
    "suppliers_with_volume": po.select("supplier_id").distinct().count(),
    "superseded_part_lines": po.join(items.filter("lifecycle_status = 'Obsolete'").select(F.col("item_id").alias("part_id")), "part_id").count(),
    "po_lines": po.count(),
    "on_time_rate": round(po.agg(F.avg(F.col("is_on_time").cast("int"))).first()[0], 3),
    "spend_by_year_bn": {r.y: r.spend_bn for r in yearly},
    "by_region": {r.region: {"lead_days": r.lead, "on_time": r.otd, "suppliers": r.suppliers} for r in by_region},
    "electronics_by_order_year": {r.y: {"lead_days": r.lead, "on_time": r.otd} for r in elec},
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
