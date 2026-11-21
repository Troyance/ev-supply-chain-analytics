# Databricks notebook source
# MAGIC %md
# MAGIC # scenarios
# MAGIC
# MAGIC tpch is a benchmark, so every supplier, part and region behaves exactly the same — which makes for a
# MAGIC boring scorecard. This notebook layers some realistic patterns on top of `fct_po_lines` and writes
# MAGIC `fct_po_lines_scenario`. The clean silver table is left alone so this can be rerun safely.
# MAGIC
# MAGIC What gets added:
# MAGIC
# MAGIC 1. every supplier gets its own reliability (some always early, some always late)
# MAGIC 2. asia ships slower (ocean freight), the americas faster (nearshore)
# MAGIC 3. air is fast, ship is slow
# MAGIC 4. a chip shortage hits electronics from mid-2021 through 2022
# MAGIC 5. a handful of suppliers start slipping in 2024 and keep getting worse
# MAGIC 6. battery materials get concentrated on one main supplier per part

# COMMAND ----------

from pyspark.sql import functions as F

SCHEMA = "workspace.ev_supply_chain"

def bucket(*cols, n):
    """deterministic 'random' number in [0, n) — same answer every run, unlike rand()"""
    return F.pmod(F.xxhash64(*cols), F.lit(n))

fct = spark.table(f"{SCHEMA}.fct_po_lines")
suppliers = spark.table(f"{SCHEMA}.dim_supplier").select("supplier_id", "region")
parts = spark.table(f"{SCHEMA}.dim_part").select("part_id", "component_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ## battery concentration
# MAGIC
# MAGIC Each part has ~4 approved suppliers. For battery materials, pick a "main" supplier per part
# MAGIC (the cheapest one) and send 90% of the lines their way.

# COMMAND ----------

main_supplier = (
    spark.table(f"{SCHEMA}.dim_part_supplier")
    .groupBy("part_id")
    .agg(F.min_by("supplier_id", "unit_cost").alias("main_supplier_id"))
)

df = (
    fct.join(parts, "part_id")
    .join(main_supplier, "part_id")
    .withColumn(
        "supplier_id",
        F.when(
            (F.col("component_category") == "Battery materials")
            & (bucket("order_id", "line_number", n=10) < 9),
            F.col("main_supplier_id"),
        ).otherwise(F.col("supplier_id")),
    )
    .join(F.broadcast(suppliers), "supplier_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## delays
# MAGIC
# MAGIC Build up an extra delay (in days) from each effect, then push the ship and receipt dates by that much.

# COMMAND ----------

months_since_2024 = F.greatest(F.lit(0), F.months_between("order_date", F.lit("2024-01-01")))
is_slipping_supplier = bucket("supplier_id", n=500) == 7  # ~0.2% of suppliers

df = (
    df
    # 1. supplier reliability: fixed offset between -12 and +12 days per supplier
    .withColumn("d_supplier", bucket("supplier_id", n=25) - 12)
    # 2. freight lane
    .withColumn("d_region", F.when(F.col("region") == "Asia", 8)
                             .when(F.col("region") == "America", -5)
                             .otherwise(0))
    # 3. ship mode
    .withColumn("d_mode", F.when(F.col("ship_mode") == "Air", -10)
                           .when(F.col("ship_mode") == "Ship", 7)
                           .otherwise(0))
    # 4. chip shortage
    .withColumn("d_shortage", F.when(
        (F.col("component_category") == "Electronics")
        & F.col("order_date").between("2021-06-01", "2022-12-31"), 21).otherwise(0))
    # 5. slipping suppliers: +1.5 days per month since jan 2024
    .withColumn("d_slip", F.when(is_slipping_supplier, F.round(months_since_2024 * 1.5)).otherwise(0))
    .withColumn("extra_delay_days",
                (F.col("d_supplier") + F.col("d_region") + F.col("d_mode")
                 + F.col("d_shortage") + F.col("d_slip")).cast("int"))
)

# COMMAND ----------

# can't ship before the order was placed, so clamp the shift
ship_date = F.greatest(F.date_add("ship_date", F.col("extra_delay_days")), F.date_add("order_date", 1))
shift_applied = F.datediff(ship_date, F.col("ship_date"))

scenario = (
    df.withColumn("shift_applied", shift_applied)
    .withColumn("ship_date", F.date_add("ship_date", F.col("shift_applied")))
    .withColumn("received_date", F.date_add("received_date", F.col("shift_applied")))
    .withColumn("lead_time_days", F.datediff("received_date", "order_date"))
    .withColumn("days_late", F.datediff("received_date", "promised_date"))
    .withColumn("is_on_time", F.col("days_late") <= 0)
    .select(fct.columns)  # same shape as the clean silver table
)

scenario.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.fct_po_lines_scenario")

# COMMAND ----------

# MAGIC %md
# MAGIC ## did it work?
# MAGIC
# MAGIC Quick before/after so we can see the patterns actually show up.

# COMMAND ----------

import json

out = spark.table(f"{SCHEMA}.fct_po_lines_scenario")

def rate(df):
    return round(df.agg(F.avg(F.col("is_on_time").cast("int"))).first()[0], 3)

supplier_rates = out.groupBy("supplier_id").agg(F.avg(F.col("is_on_time").cast("int")).alias("r"))
spread = supplier_rates.agg(F.min("r"), F.percentile_approx("r", 0.5), F.max("r")).first()

by_region = (out.join(F.broadcast(suppliers), "supplier_id")
             .groupBy("region").agg(F.round(F.avg("lead_time_days"), 1).alias("lead")).collect())

results = {
    "rows_in": fct.count(),
    "rows_out": out.count(),
    "on_time_before": rate(fct),
    "on_time_after": rate(out),
    "supplier_on_time_min_median_max": [round(x, 3) for x in spread],
    "lead_time_by_region": {r["region"]: r["lead"] for r in by_region},
    "slipping_suppliers": suppliers.filter(is_slipping_supplier).count(),
}

assert results["rows_in"] == results["rows_out"], "row count changed"
print(json.dumps(results, indent=2))
dbutils.notebook.exit(json.dumps(results))
