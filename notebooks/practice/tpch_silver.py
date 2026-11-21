# Databricks notebook source
# MAGIC %md
# MAGIC # tpch silver (spark practice)
# MAGIC
# MAGIC The first pyspark notebook in the project: cleans up the raw tpch tables into facts and dimensions. It's not
# MAGIC part of the pipeline anymore (purchasing is generated from the build schedule now), but it's a good
# MAGIC reference for joins, broadcast, lazy evaluation and writing delta tables. Reads `samples.tpch`, writes `ev_practice`.
# MAGIC
# MAGIC ## original notes
# MAGIC
# MAGIC Takes the bronze tables and turns them into clean facts and dimensions:
# MAGIC
# MAGIC - `dim_supplier` - suppliers with their country and region
# MAGIC - `dim_part` - parts, plus a made-up EV component category
# MAGIC - `dim_part_supplier` - who's approved to supply what, and at what cost
# MAGIC - `fct_po_lines` - one row per purchase order line, with lead time and lateness
# MAGIC
# MAGIC The tpch dates run 1992-1998, so everything gets shifted forward 28 years to land in 2020-2026.

# COMMAND ----------

from pyspark.sql import functions as F

SCHEMA = "workspace.ev_practice"   # scratch schema, not part of the pipeline
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
YEAR_SHIFT_MONTHS = 28 * 12

def bronze(name):
    return spark.table(f"samples.tpch.{name}")

def shift(col):
    return F.add_months(F.col(col), YEAR_SHIFT_MONTHS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_supplier
# MAGIC
# MAGIC nation and region are tiny (25 and 5 rows), so `F.broadcast` tells spark to ship a copy to every
# MAGIC worker instead of shuffling the big table around to do the join.

# COMMAND ----------

nation = bronze("nation")
region = bronze("region")

dim_supplier = (
    bronze("supplier")
    .join(F.broadcast(nation), F.col("s_nationkey") == F.col("n_nationkey"))
    .join(F.broadcast(region), F.col("n_regionkey") == F.col("r_regionkey"))
    .select(
        F.col("s_suppkey").alias("supplier_id"),
        F.trim("s_name").alias("supplier_name"),
        F.initcap("n_name").alias("country"),
        F.initcap("r_name").alias("region"),
        F.col("s_acctbal").cast("decimal(12,2)").alias("account_balance"),
    )
)

# nothing has actually run yet - spark is lazy. this is just a plan:
dim_supplier.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_part
# MAGIC
# MAGIC `p_type` looks like `ECONOMY ANODIZED STEEL`. The last word is the material, which I'm using to
# MAGIC invent an EV-ish component category so the analysis reads more naturally.

# COMMAND ----------

material = F.element_at(F.split("p_type", " "), -1)

dim_part = (
    bronze("part")
    .withColumn("material", F.initcap(material))
    .withColumn(
        "component_category",
        F.when(material == "NICKEL", "Battery materials")
         .when(material == "COPPER", "Wiring & harnesses")
         .when(material == "STEEL", "Body & chassis")
         .when(material == "TIN", "Electronics")
         .when(material == "BRASS", "Fasteners & fittings")
         .otherwise("Other"),
    )
    .select(
        F.col("p_partkey").alias("part_id"),
        F.col("p_name").alias("part_name"),
        F.initcap("p_mfgr").alias("manufacturer"),
        F.col("p_brand").alias("brand"),
        F.lower("p_type").alias("part_type"),
        "material",
        "component_category",
        F.col("p_size").alias("size"),
        F.col("p_retailprice").cast("decimal(12,2)").alias("list_price"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_part_supplier
# MAGIC
# MAGIC The approved supplier list - each part can be bought from a handful of suppliers at different costs.

# COMMAND ----------

dim_part_supplier = bronze("partsupp").select(
    F.col("ps_partkey").alias("part_id"),
    F.col("ps_suppkey").alias("supplier_id"),
    F.col("ps_supplycost").cast("decimal(12,2)").alias("unit_cost"),
    F.col("ps_availqty").alias("qty_available"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## fct_po_lines
# MAGIC
# MAGIC The big one (~30M rows). Joins line items to their order header to get the order date, then works out:
# MAGIC
# MAGIC - **lead_time_days** - order placed → goods received
# MAGIC - **days_late** - promised (commit) date → received; negative means early
# MAGIC - **is_on_time** - received on or before the promised date

# COMMAND ----------

orders = bronze("orders").select(
    F.col("o_orderkey").alias("order_id"),
    shift("o_orderdate").alias("order_date"),
    F.col("o_orderpriority").alias("order_priority"),
)

fct_po_lines = (
    bronze("lineitem")
    .select(
        F.col("l_orderkey").alias("order_id"),
        F.col("l_linenumber").alias("line_number"),
        F.col("l_partkey").alias("part_id"),
        F.col("l_suppkey").alias("supplier_id"),
        F.col("l_quantity").cast("int").alias("quantity"),
        (F.col("l_extendedprice") * (1 - F.col("l_discount"))).cast("decimal(14,2)").alias("net_amount"),
        shift("l_shipdate").alias("ship_date"),
        shift("l_commitdate").alias("promised_date"),
        shift("l_receiptdate").alias("received_date"),
        F.initcap("l_shipmode").alias("ship_mode"),
    )
    .join(orders, "order_id")
    .withColumn("lead_time_days", F.datediff("received_date", "order_date"))
    .withColumn("days_late", F.datediff("received_date", "promised_date"))
    .withColumn("is_on_time", F.col("days_late") <= 0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## write everything out
# MAGIC
# MAGIC `saveAsTable` is the *action* - this is where spark actually runs the plans above.

# COMMAND ----------

tables = {
    "dim_supplier": dim_supplier,
    "dim_part": dim_part,
    "dim_part_supplier": dim_part_supplier,
    "fct_po_lines": fct_po_lines,
}

for name, df in tables.items():
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.{name}")
    print(f"wrote {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sanity checks
# MAGIC
# MAGIC Cheap checks that catch most real-world breakage: keys are unique, nothing important is null,
# MAGIC and no rows got lost in the join.

# COMMAND ----------

import json

def dupes(table, keys):
    return spark.table(f"{SCHEMA}.{table}").groupBy(*keys).count().filter("count > 1").count()

fct = spark.table(f"{SCHEMA}.fct_po_lines")

results = {
    "rows": {name: spark.table(f"{SCHEMA}.{name}").count() for name in tables},
    "bronze_lineitem_rows": bronze("lineitem").count(),
    "dupe_suppliers": dupes("dim_supplier", ["supplier_id"]),
    "dupe_parts": dupes("dim_part", ["part_id"]),
    "dupe_po_lines": dupes("fct_po_lines", ["order_id", "line_number"]),
    "null_dates": fct.filter("order_date IS NULL OR received_date IS NULL").count(),
    "date_range": [str(d) for d in fct.agg(F.min("order_date"), F.max("received_date")).first()],
    "on_time_rate": round(fct.agg(F.avg(F.col("is_on_time").cast("int"))).first()[0], 3),
}

assert results["rows"]["fct_po_lines"] == results["bronze_lineitem_rows"], "lost rows in the orders join"
assert results["dupe_suppliers"] == results["dupe_parts"] == results["dupe_po_lines"] == 0, "duplicate keys"
assert results["null_dates"] == 0, "null dates"

print(json.dumps(results, indent=2))
dbutils.notebook.exit(json.dumps(results))
