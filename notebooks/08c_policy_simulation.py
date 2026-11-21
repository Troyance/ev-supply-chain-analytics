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
# MAGIC Writes `ev_bronze.sim_policy_daily`. dbt applies these costs in `mart_policy_comparison`:
# MAGIC - holding: 25% a year of inventory value
# MAGIC - ordering: $50 per JIT release, $150 per local order, $1,200 per overseas order (freight booking, customs, admin)
# MAGIC - shortage: plants $400 per missing part per vehicle (fitted later in the rework yard), service DC $25 + unit
# MAGIC   cost per backorder

# COMMAND ----------

import json
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

BRONZE = "workspace.ev_bronze"
SILVER = "workspace.ev_silver"
GOLD = "workspace.ev_gold"
SIM_START = "2025-11-17"          # 52 weeks ending with the last full week of history
WARMUP_WEEKS = 13
HOLDING_RATE = 0.25
POLICIES = ["current", "lean", "high_service", "eoq", "ddmrp", "mrp"]

# reads the dbt models, writes a raw simulation output that dbt costs out (mart_policy_comparison)
supplier = spark.table(f"{GOLD}.mart_supplier_scorecard").select(
    "supplier_key", "supplier_id", F.col("region").alias("supplier_region"), F.col("tier").alias("supplier_tier"),
    "on_time_rate", F.coalesce("avg_days_late_when_late", F.lit(0.0)).alias("days_late_when_late"),
    F.col("lead_time_stddev_days").alias("actual_lt_stddev"))
planning = (spark.table(f"{SILVER}.dim_part")
            .join(spark.table(f"{SILVER}.bridge_part_supplier").filter("sourcing_role = 'primary'").select("part_key", "supplier_key"), "part_key")
            .join(supplier, "supplier_key")
            .select("part_id", "component_category", "supplier_id", "supplier_region", "supplier_tier",
                    F.col("standard_cost").alias("unit_cost"), "buffer_class", "planning_lead_time_days",
                    "lead_time_stddev_days", "review_period_days", "service_level_z", "pack_size", "moq",
                    "on_time_rate", "days_late_when_late", "actual_lt_stddev"))
qpv = spark.table(f"{SILVER}.bridge_vehicle_model_part").groupBy("part_id").agg(F.avg("qty_per_vehicle").alias("qty_per_vehicle"))

demand = (spark.table(f"{SILVER}.fct_part_demand").select("week_start", "site_id", "part_id", "units")
          .filter(F.col("week_start") >= F.date_sub(F.lit(SIM_START).cast("date"), WARMUP_WEEKS * 7)))

inputs = (demand
          .join(planning, "part_id")
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
daily.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{BRONZE}.sim_policy_daily")
daily = spark.table(f"{BRONZE}.sim_policy_daily")

# COMMAND ----------

# costs (holding, ordering, shortage) are worked out in dbt: mart_policy_comparison. this is just a quick check
by_part = (daily.groupBy("policy", "site_id", "part_id")
           .agg(F.sum("demand").alias("d"), F.sum("unmet").alias("u"), F.sum((F.col("unmet") > 0).cast("int")).alias("so")))
rows = (by_part.groupBy("policy")
        .agg(F.round(1 - F.sum("u") / F.sum("d"), 4).alias("unit_fill_rate"),
             F.round(F.avg((F.col("so") > 0).cast("int")), 3).alias("share_of_parts_with_stockout"))
        .orderBy("share_of_parts_with_stockout").collect())
result = {"parts_simulated": by_part.select("site_id", "part_id").distinct().count(),
          "by_policy": {r.policy: {"unit_fill_rate": r.unit_fill_rate, "share_of_parts_with_stockout": r.share_of_parts_with_stockout}
                        for r in rows}}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
