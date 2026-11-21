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
# MAGIC Output: `ev_bronze.sim_inventory_projection`, one row per site, part and day. dbt builds the per-part summary.

# COMMAND ----------

import json
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

BRONZE = "workspace.ev_bronze"
GOLD = "workspace.ev_gold"
HORIZON_DAYS = 91

# reads the dbt marts, writes a raw simulation output that dbt picks up (tag: simulation)
signals = spark.table(f"{GOLD}.mart_buy_signals").filter("daily_run_rate > 0")
# the marts store quantities as decimal(18,2) - pandas wants floats
plan = spark.table(f"{GOLD}.mart_part_demand_plan").select("site_id", "part_id", "target_week", F.col("demand_p50").cast("double").alias("demand_p50"))
reliability = spark.table(f"{GOLD}.mart_supplier_scorecard").select(
    "supplier_key", "on_time_rate", F.coalesce("avg_days_late_when_late", F.lit(0.0)).alias("days_late_when_late"))

inputs = (plan.join(signals.select("site_id", "part_id", "snapshot_date", "supplier_key", "safety_stock", "reorder_point",
                                   "order_up_to", "on_hand_qty", "on_order_qty", "planning_lead_time_days",
                                   "review_period_days", "pack_size", "moq", F.col("unit_price").cast("double").alias("unit_cost")),
                    ["site_id", "part_id"])
          .join(reliability, "supplier_key", "left"))

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
projection.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{BRONZE}.sim_inventory_projection")
projection = spark.table(f"{BRONZE}.sim_inventory_projection")

# COMMAND ----------

# the per-part summary (lowest point, stockout days, fill rate) is built in dbt: mart_inventory_projection_summary
by_part = projection.groupBy("site_id", "part_id").agg(F.sum((F.col("on_hand") <= 0).cast("int")).alias("stockout_days"))
result = {"projection_rows": projection.count(), "parts": by_part.count(),
          "parts_with_projected_stockout": by_part.filter("stockout_days > 0").count()}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
