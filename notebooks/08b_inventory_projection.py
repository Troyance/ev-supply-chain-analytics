# Databricks notebook source
# MAGIC %md
# MAGIC # inventory projection (sawtooth)
# MAGIC
# MAGIC The buy signals are a snapshot. This plays each site x part forward day by day for the 13-week forecast
# MAGIC horizon using the same replenishment policy, so you can see the sawtooth: stock drains at the forecast
# MAGIC run rate, orders fire when inventory position drops below the reorder point, and land a lead time later.
# MAGIC
# MAGIC Each order's arrival is pushed back by a realistic amount if that supplier tends to be late (drawn from
# MAGIC their actual on-time rate and lateness in the scorecard), so unreliable suppliers show up as dips.
# MAGIC
# MAGIC Spark-wise this is a grouped map: `groupBy(site, part).applyInPandas(simulate)` runs one small pandas
# MAGIC simulation per part, spread across the workers.
# MAGIC
# MAGIC Outputs:
# MAGIC - `gold_inventory_projection` – one row per site, part and day
# MAGIC - `gold_inventory_projection_summary` – one row per site and part: lowest point, stockout days, fill rate

# COMMAND ----------

import json
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

SCHEMA = "workspace.ev_supply_chain"
HORIZON_DAYS = 91

signals = spark.table(f"{SCHEMA}.gold_buy_signals").filter("daily_run_rate > 0")
plan = spark.table(f"{SCHEMA}.gold_part_demand_plan").select("site_id", "part_id", "target_week", "demand_p50")
reliability = spark.table(f"{SCHEMA}.gold_supplier_scorecard").select(
    "supplier_id", (F.col("on_time_pct") / 100).alias("on_time_rate"),
    F.coalesce("avg_days_late_when_late", F.lit(0.0)).alias("days_late_when_late"))

inputs = (plan.join(signals.select("site_id", "part_id", "snapshot_date", "supplier_id", "safety_stock", "reorder_point",
                                   "order_up_to", "on_hand_qty", "on_order_qty", "planning_lead_time_days",
                                   "review_period_days", "pack_size", "moq", "unit_cost"), ["site_id", "part_id"])
          .join(reliability, "supplier_id", "left"))

# COMMAND ----------

OUT_SCHEMA = """site_id string, part_id bigint, day date, day_index int, demand double, receipts double,
                order_placed double, on_hand double, inventory_position double, unmet_demand double,
                safety_stock double, reorder_point double, unit_cost double"""

def simulate(df: pd.DataFrame) -> pd.DataFrame:
    p = df.iloc[0]
    site, part = p["site_id"], int(p["part_id"])
    start = pd.Timestamp(p["snapshot_date"])
    lt, review, pack = int(p["planning_lead_time_days"]), int(p["review_period_days"]), max(int(p["pack_size"]), 1)
    rop, up_to, ss = float(p["reorder_point"]), float(p["order_up_to"]), float(p["safety_stock"])
    moq = float(p["moq"])
    on_time = float(p["on_time_rate"]) if pd.notna(p["on_time_rate"]) else 1.0
    late_days = float(p["days_late_when_late"]) if pd.notna(p["days_late_when_late"]) else 0.0
    rng = np.random.default_rng(part * 7 + hash(site) % 1000)   # same answer every run

    # daily demand: weekly forecast spread over the week
    weekly = df.set_index(pd.to_datetime(df["target_week"]))["demand_p50"].sort_index()
    days = pd.date_range(start, periods=HORIZON_DAYS, freq="D")
    week_start = days - pd.to_timedelta(days.weekday, unit="D")
    demand = weekly.reindex(week_start).ffill().bfill().to_numpy() / 7

    # what's already on order arrives evenly over the next lead time
    receipts = np.zeros(HORIZON_DAYS + lt + 200)
    if p["on_order_qty"] > 0:
        receipts[1:lt + 1] += float(p["on_order_qty"]) / max(lt, 1)

    on_hand, open_orders = float(p["on_hand_qty"]), float(p["on_order_qty"])
    first_review = (part + hash(site)) % review          # stagger review days across parts
    rows = []
    for d in range(HORIZON_DAYS):
        arrived = receipts[d]
        on_hand += arrived
        open_orders = max(open_orders - arrived, 0.0)

        use = demand[d]
        unmet = max(use - on_hand, 0.0)
        on_hand = max(on_hand - use, 0.0)

        placed = 0.0
        if (d - first_review) % review == 0 and on_hand + open_orders < rop:
            placed = max(np.ceil((up_to - on_hand - open_orders) / pack) * pack, moq)
            delay = int(round(rng.exponential(late_days))) if rng.random() > on_time else 0
            receipts[d + lt + delay] += placed
            open_orders += placed

        rows.append((site, part, days[d].date(), d, use, arrived, placed, on_hand, on_hand + open_orders,
                     unmet, ss, rop, float(p["unit_cost"])))
    return pd.DataFrame(rows, columns=[c.split()[0] for c in OUT_SCHEMA.replace("\n", " ").split(",")])

projection = inputs.groupBy("site_id", "part_id").applyInPandas(simulate, OUT_SCHEMA)
projection.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.gold_inventory_projection")
projection = spark.table(f"{SCHEMA}.gold_inventory_projection")

# COMMAND ----------

# MAGIC %md
# MAGIC ## summary per part

# COMMAND ----------

summary = (projection.groupBy("site_id", "part_id")
           .agg(F.min("on_hand").alias("min_on_hand"),
                F.sum((F.col("on_hand") <= 0).cast("int")).alias("stockout_days"),
                F.min(F.when(F.col("on_hand") <= 0, F.col("day"))).alias("first_stockout_date"),
                F.sum((F.col("on_hand") < F.col("safety_stock")).cast("int")).alias("days_below_safety_stock"),
                F.sum((F.col("order_placed") > 0).cast("int")).alias("orders_placed"),
                F.avg(F.col("on_hand") * F.col("unit_cost")).alias("avg_inventory_value"),
                (1 - F.sum("unmet_demand") / F.sum("demand")).alias("fill_rate"))
           .join(spark.table(f"{SCHEMA}.gold_buy_signals").select(
               "site_id", "part_id", "part_number", "part_name", "component_category", "buffer_class",
               "supplier_name", "supplier_tier", "signal"), ["site_id", "part_id"]))

summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.gold_inventory_projection_summary")
summary = spark.table(f"{SCHEMA}.gold_inventory_projection_summary")

by_class = (summary.groupBy("buffer_class")
            .agg(F.count("*").alias("parts"), F.sum((F.col("stockout_days") > 0).cast("int")).alias("parts_with_stockout"),
                 F.round(F.sum(F.col("fill_rate") * 1) / F.count("*"), 4).alias("avg_fill_rate"),
                 F.round(F.avg("orders_placed"), 1).alias("avg_orders")).collect())
result = {
    "projection_rows": projection.count(),
    "parts": summary.count(),
    "by_buffer_class": {r.buffer_class: {"parts": r.parts, "parts_with_stockout": r.parts_with_stockout,
                                         "avg_fill_rate": r.avg_fill_rate, "avg_orders": r.avg_orders} for r in by_class},
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
