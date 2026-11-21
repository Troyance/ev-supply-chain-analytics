# Databricks notebook source
# MAGIC %md
# MAGIC # purchasing
# MAGIC
# MAGIC Generates PO history the way it actually comes about: from the build schedule. Every week, each BOM part
# MAGIC needs (builds x qty per vehicle) units at each plant, and the part's supplier delivers against that.
# MAGIC
# MAGIC tpch still provides the supplier master (names, countries, regions). Everything else is calibrated to
# MAGIC look like a real EV maker:
# MAGIC
# MAGIC - ~840 active suppliers, mostly north america, then asia and europe
# MAGIC - category / region fit: battery and electronics mostly from asia, body and chassis mostly local
# MAGIC - ~85% of parts single-sourced (normal in auto - tooling is expensive), the rest dual-sourced
# MAGIC - quoted lead times by region, ~94% on-time on average, lateness in days not weeks
# MAGIC - the 2021-22 chip shortage hits electronics: longer quoted lead times and a lot more late deliveries
# MAGIC - a handful of suppliers start slipping in 2024 and keep getting worse
# MAGIC
# MAGIC Outputs to `ev_bronze`: `erp_suppliers`, `erp_sourcing`, `erp_po_deliveries`. The supplier table also holds each
# MAGIC supplier's true reliability (p_late, is_slipping). dbt staging drops those - analytics has to find them.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## active suppliers
# MAGIC
# MAGIC Keep a region-weighted slice of the 50k tpch suppliers, and give each one a quoted lead time,
# MAGIC a reliability, and a typical lateness when they are late.

# COMMAND ----------

keep_one_in = F.when(region == "America", 20).when(region == "Asia", 50).when(region == "Europe", 100).otherwise(500)

supplier_master = (spark.table(f"{BRONZE}.tpch_supplier")
                   .join(spark.table(f"{BRONZE}.tpch_nation"), F.col("s_nationkey") == F.col("n_nationkey"))
                   .join(spark.table(f"{BRONZE}.tpch_region"), F.col("n_regionkey") == F.col("r_regionkey"))
                   .select(F.col("s_suppkey").alias("supplier_id"), F.trim("s_name").alias("supplier_name"),
                           F.initcap("n_name").alias("country"), F.initcap("r_name").alias("region")))

active = (supplier_master
          .filter(bucket("supplier_id", n=1_000_000, salt="active") * keep_one_in < 1_000_000)
          .withColumn("u", uniform("supplier_id", salt="profile")))

active = save(
    active
    .withColumn("quoted_lead_time_days",
                (F.when(region == "America", 14 + bucket("supplier_id", n=15, salt="lt"))
                  .when(region == "Europe", 42 + bucket("supplier_id", n=15, salt="lt"))
                  .when(region == "Asia", 70 + bucket("supplier_id", n=29, salt="lt"))
                  .otherwise(56 + bucket("supplier_id", n=22, salt="lt"))).cast("int"))
    .withColumn("transit_days", F.when(region == "America", 3).when(region == "Europe", 21)
                                 .when(region == "Asia", 35).otherwise(28))
    # most suppliers are good, a few are bad: late 1% to 19% of the time, ~6% on average
    .withColumn("p_late", 0.01 + 0.18 * F.pow("u", 2.5))
    .withColumn("late_mean_days", (F.when(region == "America", 2).when(region == "Europe", 4)
                                    .when(region == "Asia", 6).otherwise(5))
                                  * (0.7 + 0.6 * uniform("supplier_id", salt="late")))
    .withColumn("is_slipping", bucket("supplier_id", n=100, salt="slip") == 0)
    .withColumn("region_idx", F.row_number().over(Window.partitionBy("region").orderBy(F.xxhash64("supplier_id"))) - 1)
    .drop("u"),
    "erp_suppliers")

region_counts = active.groupBy("region").count().withColumnRenamed("count", "region_n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sourcing
# MAGIC
# MAGIC Pick a region for each part based on its category, then a supplier in that region. 15% of non-battery
# MAGIC parts also get a north american second source taking 30% of the volume.

# COMMAND ----------

REGION_MIX = {  # category -> [(region, share)]
    "Battery materials":    [("Asia", .70), ("America", .20), ("Europe", .10)],
    "Electronics":          [("Asia", .60), ("America", .25), ("Europe", .15)],
    "Wiring & harnesses":   [("America", .65), ("Asia", .25), ("Europe", .10)],
    "Body & chassis":       [("America", .85), ("Europe", .10), ("Asia", .05)],
    "Fasteners & fittings": [("America", .52), ("Asia", .33), ("Europe", .10), ("Middle East", .03), ("Africa", .02)],
}

u = uniform("part_id", salt="region")
pick_region = None
for cat, mix in REGION_MIX.items():
    cum, expr = 0.0, None
    for r, share in mix:
        cum += share
        expr = F.when(u < cum, r) if expr is None else expr.when(u < cum, r)
    cond = F.col("component_category") == cat
    pick_region = F.when(cond, expr) if pick_region is None else pick_region.when(cond, expr)

catalog = spark.table(f"{BRONZE}.erp_parts")

def assign(df, salt):
    return (df.join(region_counts, "region")
              .withColumn("region_idx", bucket("part_id", n=F.col("region_n"), salt=salt).cast("int"))
              .join(active.select("supplier_id", "region", "region_idx"), ["region", "region_idx"]))

primary = (assign(catalog.withColumn("region", pick_region), "primary")
           .select("part_id", "supplier_id", F.lit("primary").alias("sourcing_role"), "standard_cost"))

is_dual = (F.col("component_category") != "Battery materials") & (uniform("part_id", salt="dual") < 0.15)
secondary = (assign(catalog.filter(is_dual).withColumn("region", F.lit("America")), "secondary")
             .select("part_id", F.col("supplier_id").alias("secondary_id")))

dual = primary.join(secondary, "part_id").filter("supplier_id != secondary_id").select("part_id", "secondary_id")

sourcing = save(
    primary.join(dual, "part_id", "left")
    .withColumn("volume_share", F.when(F.col("secondary_id").isNotNull(), 0.7).otherwise(1.0))
    .select("part_id", "supplier_id", "sourcing_role", "volume_share",
            F.col("standard_cost").alias("unit_price"))
    .unionByName(
        dual.join(catalog.select("part_id", "standard_cost"), "part_id")
        .select("part_id", F.col("secondary_id").alias("supplier_id"), F.lit("secondary").alias("sourcing_role"),
                F.lit(0.3).alias("volume_share"),
                (F.col("standard_cost") * 1.08).cast("decimal(14,4)").alias("unit_price"))),  # backup costs a bit more
    "erp_sourcing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## delivery lines
# MAGIC
# MAGIC Weekly requirement per plant and part = builds x qty per vehicle. Local body and wiring suppliers deliver
# MAGIC daily (5 lines a week), everyone else weekly. Each line was ordered one quoted lead time before it was
# MAGIC promised, and arrives on time or a few days early - unless the supplier is late.

# COMMAND ----------

requirements = (spark.table(f"{BRONZE}.mes_vehicle_builds")
                .join(spark.table(f"{BRONZE}.erp_bom"), "model_id")
                .groupBy("site_id", "part_id", "week_start")
                .agg(F.sum(F.col("units") * F.col("qty_per_vehicle")).alias("weekly_qty")))

lines = (requirements
         .join(sourcing, "part_id")
         .join(catalog.select("part_id", "component_category"), "part_id")
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
    "active_suppliers": active.count(),
    "suppliers_with_volume": po.select("supplier_id").distinct().count(),
    "parts_dual_sourced": sourcing.filter("sourcing_role = 'secondary'").count(),
    "po_lines": po.count(),
    "on_time_rate": round(po.agg(F.avg(F.col("is_on_time").cast("int"))).first()[0], 3),
    "spend_by_year_bn": {r.y: r.spend_bn for r in yearly},
    "by_region": {r.region: {"lead_days": r.lead, "on_time": r.otd, "suppliers": r.suppliers} for r in by_region},
    "electronics_by_order_year": {r.y: {"lead_days": r.lead, "on_time": r.otd} for r in elec},
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
