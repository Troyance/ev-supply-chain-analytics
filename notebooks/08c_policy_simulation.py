# Databricks notebook source
# MAGIC %md
# MAGIC # ordering policy simulation
# MAGIC
# MAGIC Replays the last 52 weeks of actual demand for every site x part under five ordering policies and adds
# MAGIC up what each one costs. Same demand, same supplier lateness on the same days (common random numbers),
# MAGIC same starting point - so differences come from the policy, not luck.
# MAGIC
# MAGIC | policy | how it orders |
# MAGIC |---|---|
# MAGIC | current | reorder point + order-up-to, reviewed every R days, safety stock by buffer class |
# MAGIC | lean | same, half the safety stock |
# MAGIC | high_service | same, but 99% service level for every part using actual supplier lead time variability |
# MAGIC | eoq | checked daily, order the economic order quantity when below the reorder point |
# MAGIC | ddmrp | red / yellow / green buffers from ADU, lead time factor and variability factor, checked daily |
# MAGIC | mrp | same rules as current, but sized from the *scheduled* usage over the next lead time + review period. plant demand comes from a firm build schedule, so planners know it ahead. the service DC has no schedule, so it falls back to the trailing average |
# MAGIC
# MAGIC Every policy uses ADU (average daily usage, trailing 13 weeks) the way a planner would, and every order
# MAGIC respects the MOQ and pack size.
# MAGIC
# MAGIC Costs:
# MAGIC - holding: 25% a year of inventory value
# MAGIC - ordering: $50 per JIT release, $150 per local order, $1,200 per overseas order (freight booking, customs, admin)
# MAGIC - shortage: plants $400 per missing part per vehicle (fitted later in the rework yard), service DC $25 + unit
# MAGIC   cost per backorder

# COMMAND ----------

import json
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

SCHEMA = "workspace.ev_supply_chain"
SIM_START = "2025-11-17"          # 52 weeks ending with the last full week of history
WARMUP_WEEKS = 13
HOLDING_RATE = 0.25
POLICIES = ["current", "lean", "high_service", "eoq", "ddmrp", "mrp"]

planning = spark.table(f"{SCHEMA}.dim_part_planning")
scorecard = spark.table(f"{SCHEMA}.gold_supplier_scorecard").select(
    "supplier_id", (F.col("on_time_pct") / 100).alias("on_time_rate"),
    F.coalesce("avg_days_late_when_late", F.lit(0.0)).alias("days_late_when_late"),
    F.col("lead_time_stddev").alias("actual_lt_stddev"))
qpv = spark.table(f"{SCHEMA}.dim_bom").groupBy("part_id").agg(F.avg("qty_per_vehicle").alias("qty_per_vehicle"))

demand = (spark.table(f"{SCHEMA}.fct_part_demand_weekly")
          .filter(F.col("week_start") >= F.date_sub(F.lit(SIM_START).cast("date"), WARMUP_WEEKS * 7)))

inputs = (demand
          .join(planning.select("part_id", "component_category", "supplier_id", "supplier_region", "supplier_tier",
                                "unit_cost", "buffer_class", "planning_lead_time_days", "lead_time_stddev_days",
                                "review_period_days", "service_level_z", "pack_size", "moq"), "part_id")
          .join(scorecard, "supplier_id", "left")
          .join(qpv, "part_id", "left"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## the simulation
# MAGIC
# MAGIC One pandas function per site x part. Loops day by day: receive what arrives, consume demand (anything
# MAGIC that can't be covered is a shortage), then check the policy and place an order if it says so.

# COMMAND ----------

OUT_SCHEMA = """site_id string, part_id bigint, policy string, day date, demand double, on_hand double,
                receipts double, order_qty double, unmet double"""

def simulate(df: pd.DataFrame) -> pd.DataFrame:
    p = df.iloc[0]
    site, part = p["site_id"], int(p["part_id"])
    cls, lt, review = p["buffer_class"], int(p["planning_lead_time_days"]), int(p["review_period_days"])
    pack, moq, cost = max(int(p["pack_size"]), 1), float(p["moq"]), float(p["unit_cost"])
    z, lt_sd = float(p["service_level_z"]), float(p["lead_time_stddev_days"])
    actual_lt_sd = float(p["actual_lt_stddev"]) if pd.notna(p["actual_lt_stddev"]) else lt_sd
    on_time = float(p["on_time_rate"]) if pd.notna(p["on_time_rate"]) else 1.0
    late_days = float(p["days_late_when_late"]) if pd.notna(p["days_late_when_late"]) else 0.0
    overseas = p["supplier_region"] != "America"
    order_cost = 50.0 if cls == "JIT" else (1200.0 if overseas else 150.0)

    # daily demand from weekly buckets
    weekly = df.groupby("week_start")["units"].sum().sort_index()
    weekly.index = pd.to_datetime(weekly.index)
    # pad with zero demand back to the warmup start, so lines that launched mid-window (plant 2) still get simulated
    warm_start = pd.Timestamp(SIM_START) - pd.Timedelta(weeks=WARMUP_WEEKS)
    weekly = weekly.reindex(pd.date_range(min(warm_start, weekly.index.min()), weekly.index.max(), freq="W-MON"), fill_value=0)
    full = pd.date_range(weekly.index.min(), weekly.index.max() + pd.Timedelta(days=6), freq="D")
    daily = weekly.reindex(full - pd.to_timedelta(full.weekday, unit="D")).fillna(0).to_numpy() / 7
    start = int(np.searchsorted(full, pd.Timestamp(SIM_START)))
    if start < 28 or daily[start:].sum() <= 0:
        return pd.DataFrame(columns=[c.split()[0] for c in OUT_SCHEMA.replace("\n", " ").split(",")])
    days = full[start:]
    n = len(days)

    # what a planner knows each day: trailing 13-week average and weekly std
    adu = pd.Series(daily).rolling(91, min_periods=28).mean().shift(1).fillna(0).to_numpy()[start:]
    wk_sd = weekly.rolling(13, min_periods=4).std().shift(1)
    sd_daily = (wk_sd.reindex(days - pd.to_timedelta(days.weekday, unit="D")).ffill().fillna(0).to_numpy()) / np.sqrt(7)
    demand = daily[start:]

    # mrp: average scheduled usage over the coming lead time + review period (the build schedule is known)
    horizon = lt + max(review, 1)
    padded = np.concatenate([daily, np.repeat(daily[-7:].mean(), horizon)])
    csum = np.concatenate([[0], np.cumsum(padded)])
    fwd = np.array([(csum[i + horizon] - csum[i]) / horizon for i in range(start, start + len(days))]) \
        if site != "SDC1" else adu

    # common random numbers: the same late deliveries for every policy
    rng = np.random.default_rng(part * 31 + sum(map(ord, site)))
    late_draw, late_size = rng.random(n), rng.exponential(1.0, n)

    def ss_for(policy, a, sd):
        if policy == "high_service":
            return 2.33 * np.sqrt(lt * sd ** 2 + (a * actual_lt_sd) ** 2)
        if cls == "JIT":
            base = 0.5 * a
        elif cls == "KANBAN":
            base = 3 * a
        else:
            base = z * np.sqrt(lt * sd ** 2 + (a * lt_sd) ** 2)
        return base * (0.5 if policy == "lean" else 1.0)

    def round_up(q):
        return max(np.ceil(q / pack) * pack, moq)

    out = []
    for policy in POLICIES:
        a0, sd0 = (fwd[0] if policy == "mrp" else adu[0]), sd_daily[0]
        ss0 = ss_for("current" if policy in ("eoq", "ddmrp", "mrp") else policy, a0, sd0)
        on_hand = ss0 + a0 * max(review, 1) / 2
        receipts = np.zeros(n + lt + 400)
        receipts[1:lt + 1] += a0                       # steady-state pipeline: one lead time of usage in transit
        open_orders = a0 * lt

        for d in range(n):
            arrived = receipts[d]
            on_hand += arrived
            open_orders = max(open_orders - arrived, 0.0)
            use = demand[d]
            unmet = max(use - on_hand, 0.0)
            on_hand = max(on_hand - use, 0.0)

            a, sd = (fwd[d] if policy == "mrp" else adu[d]), sd_daily[d]
            ip = on_hand + open_orders
            q = 0.0
            if policy in ("current", "lean", "high_service", "mrp"):
                if d % review == 0:
                    ss = ss_for(policy, a, sd)
                    rop = a * (lt + review) + ss
                    if ip < rop:
                        q = round_up(rop + a * review - ip)
            elif policy == "eoq":
                s = a * lt + ss_for("current", a, sd)
                if ip < s:
                    annual = a * 365
                    eoq = np.sqrt(2 * annual * order_cost / (HOLDING_RATE * cost)) if cost > 0 else moq
                    q = round_up(max(eoq, s - ip))
            else:  # ddmrp
                ltf = 0.7 if lt <= 10 else (0.5 if lt <= 40 else 0.3)
                vf = {"A": 0.3, "B": 0.5}.get(p["supplier_tier"], 0.7)
                red = a * lt * ltf * (1 + vf)
                yellow = a * lt
                green = max(moq, a * max(review, 1), a * lt * ltf)
                if ip <= red + yellow:
                    q = round_up(red + yellow + green - ip)

            if q > 0:
                delay = int(round(late_size[d] * late_days)) if late_draw[d] > on_time else 0
                receipts[d + lt + delay] += q
                open_orders += q
            out.append((site, part, policy, days[d].date(), use, on_hand, arrived, q, unmet))

    return pd.DataFrame(out, columns=[c.split()[0] for c in OUT_SCHEMA.replace("\n", " ").split(",")])

daily = inputs.groupBy("site_id", "part_id").applyInPandas(simulate, OUT_SCHEMA)
daily.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.gold_policy_simulation_daily")
daily = spark.table(f"{SCHEMA}.gold_policy_simulation_daily")

# COMMAND ----------

# MAGIC %md
# MAGIC ## cost it out

# COMMAND ----------

attrs = (planning.select("part_id", "part_number", "part_name", "component_category", "buffer_class", "supplier_name",
                         "supplier_region", "supplier_tier", "unit_cost", "moq")
         .join(qpv, "part_id", "left"))

overseas = F.col("supplier_region") != "America"
order_cost = F.when(F.col("buffer_class") == "JIT", 50.0).when(overseas, 1200.0).otherwise(150.0)
shortage_cost_per_unit = F.when(F.col("site_id") == "SDC1", 25 + F.col("unit_cost"))\
                          .otherwise(400.0 / F.coalesce(F.col("qty_per_vehicle"), F.lit(1.0)))

comparison = (daily.groupBy("site_id", "part_id", "policy")
              .agg(F.sum("demand").alias("demand_units"), F.sum("unmet").alias("units_short"),
                   F.sum((F.col("unmet") > 0).cast("int")).alias("stockout_days"),
                   F.sum((F.col("order_qty") > 0).cast("int")).alias("orders_placed"),
                   F.avg("on_hand").alias("avg_on_hand"), F.max("on_hand").alias("max_on_hand"))
              .join(attrs, "part_id")
              .withColumn("avg_inventory_value", F.col("avg_on_hand") * F.col("unit_cost"))
              .withColumn("holding_cost", F.col("avg_inventory_value") * HOLDING_RATE)
              .withColumn("ordering_cost", F.col("orders_placed") * order_cost)
              .withColumn("shortage_cost", F.col("units_short") * shortage_cost_per_unit)
              .withColumn("total_cost", F.col("holding_cost") + F.col("ordering_cost") + F.col("shortage_cost"))
              .withColumn("fill_rate", 1 - F.col("units_short") / F.col("demand_units")))

comparison.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.gold_policy_comparison")
comparison = spark.table(f"{SCHEMA}.gold_policy_comparison")

# COMMAND ----------

rows = (comparison.groupBy("policy")
        .agg(F.round(F.sum("holding_cost") / 1e6, 2).alias("holding_m"), F.round(F.sum("ordering_cost") / 1e6, 2).alias("ordering_m"),
             F.round(F.sum("shortage_cost") / 1e6, 2).alias("shortage_m"), F.round(F.sum("total_cost") / 1e6, 2).alias("total_m"),
             F.round(F.sum("avg_inventory_value") / 1e6, 1).alias("avg_inventory_m"),
             F.round(1 - F.sum("units_short") / F.sum("demand_units"), 4).alias("fill_rate"),
             F.sum("orders_placed").alias("orders"))
        .orderBy("total_m").collect())
result = {"parts_simulated": comparison.select("site_id", "part_id").distinct().count(),
          "by_policy": {r.policy: {k: r[k] for k in ["holding_m", "ordering_m", "shortage_m", "total_m",
                                                      "avg_inventory_m", "fill_rate", "orders"]} for r in rows}}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
