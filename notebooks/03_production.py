# Databricks notebook source
# MAGIC %md
# MAGIC # production
# MAGIC
# MAGIC The build side of the world. Nothing here depends on suppliers, so it runs before purchasing.
# MAGIC
# MAGIC - `dim_site` - two plants and a service parts DC
# MAGIC - `dim_vehicle_model` - three vehicle models
# MAGIC - `fct_vehicle_builds` - weekly builds per plant/model (ramps, launches, shutdowns, a chip shortage)
# MAGIC - `dim_part_catalog` - the ~1,900 parts we actually build with: part number, a real name, standard cost
# MAGIC - `dim_bom` - which parts go into which vehicle, and how many
# MAGIC
# MAGIC Standard costs are calibrated so a vehicle's BOM comes to about $45k, split across categories roughly
# MAGIC like a real EV (battery biggest, fasteners tiny).

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

SCHEMA = "workspace.ev_supply_chain"
rng = np.random.default_rng(42)
weeks = pd.date_range("2020-01-06", "2026-11-09", freq="W-MON")

def save(pdf_or_sdf, name):
    sdf = spark.createDataFrame(pdf_or_sdf) if isinstance(pdf_or_sdf, pd.DataFrame) else pdf_or_sdf
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.{name}")
    return spark.table(f"{SCHEMA}.{name}")

def bucket(*cols, n, salt):
    """deterministic 'random' int in [0, n). the salt keeps different draws independent"""
    return F.pmod(F.xxhash64(F.lit(salt), *cols), F.lit(n))

def uniform(*cols, salt):
    return bucket(*cols, n=1_000_000, salt=salt) / 1_000_000

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
# MAGIC Each line launches, ramps on an S-curve, then grows slowly. On top of that: summer changeover shutdown,
# MAGIC holiday shutdown, end-of-quarter push, and the 2021-22 chip shortage (gradual onset, worst in spring 2022,
# MAGIC recovered by year end). Plant 2 launched in March 2026
# MAGIC so it's still mid-ramp.

# COMMAND ----------

# (site, model, launch week, peak weekly units, weeks to ramp)
lines = [
    ("PLT1", "PU",  "2021-09-13", 600, 70),
    ("PLT1", "SUV", "2021-12-06", 700, 70),
    ("PLT1", "VAN", "2022-03-07", 400, 60),
    ("PLT2", "SUV", "2026-03-02", 500, 52),
]

# chip shortage: builds up from jun 2021, worst in apr 2022, fully recovered by dec 2022.
# purchasing uses the same window for electronics lead times.
SHORTAGE_START, SHORTAGE_PEAK, SHORTAGE_END = pd.Timestamp("2021-06-01"), pd.Timestamp("2022-04-01"), pd.Timestamp("2022-12-31")

def shortage_severity(week):
    """0 outside the shortage, rising linearly to 1 at the peak, back to 0 at the end"""
    if week <= SHORTAGE_START or week >= SHORTAGE_END:
        return 0.0
    if week <= SHORTAGE_PEAK:
        return (week - SHORTAGE_START) / (SHORTAGE_PEAK - SHORTAGE_START)
    return (SHORTAGE_END - week) / (SHORTAGE_END - SHORTAGE_PEAK)

def seasonality(week):
    woy = week.isocalendar()[1]
    m = 1.0
    if woy in (27, 28):
        m *= 0.3                                  # summer model-year changeover
    if woy >= 52 or woy == 1:
        m *= 0.5                                  # holiday shutdown
    if week.month in (3, 6, 9, 12) and week.day >= 16:
        m *= 1.12                                 # end of quarter push
    m *= 1 - 0.30 * shortage_severity(week)       # chip shortage, up to -30% at the worst point
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
# MAGIC ## part catalog
# MAGIC
# MAGIC tpch part names are random color words ("navy smoke lavender red burnished") and every part costs about
# MAGIC the same. A real BOM is a few expensive modules and a long tail of cheap brackets, clips and fasteners.
# MAGIC
# MAGIC Each category has a list of part types: (name, typical price, how common it is, qty per vehicle).
# MAGIC Every catalog part draws a type (cheap types are much more common), a variant suffix, and a price
# MAGIC around the typical one. At the end one scale factor nudges the total to about $45k per vehicle.

# COMMAND ----------

# share of part numbers by category - most of a BOM is small stuff
CATEGORY_MIX = {"Fasteners & fittings": 0.40, "Body & chassis": 0.27, "Electronics": 0.10,
                "Wiring & harnesses": 0.12, "Battery materials": 0.11}

# (name, typical unit price $, relative frequency, qty per vehicle low, high)
PART_TYPES = {
    "Battery materials": ("BAT", ["", "Rev B", "Gen 2"], [
        ("Cell module", 2200, 2, 4, 6), ("Pack lid", 650, 1, 1, 1), ("HV contactor", 120, 2, 2, 3),
        ("Cooling plate", 85, 4, 2, 6), ("Coolant manifold", 60, 3, 1, 2), ("Pack fuse", 45, 2, 1, 2),
        ("Pack enclosure seal", 22, 6, 1, 2), ("Pack vent valve", 18, 4, 1, 2), ("Module end plate", 14, 8, 8, 12),
        ("Busbar", 9, 15, 8, 24), ("Thermal barrier sheet", 6, 10, 6, 12), ("Module bracket", 5, 10, 8, 16),
        ("Thermal pad", 4, 12, 8, 20), ("Cell interconnect", 3, 15, 20, 60), ("Compression strap", 2.5, 10, 8, 16)]),
    "Electronics": ("ELE", ["", "Rev B", "Rev C", "Gen 2"], [
        ("Drive inverter", 1400, 1, 1, 2), ("Infotainment head unit", 900, 1, 1, 1), ("Onboard charger", 650, 1, 1, 1),
        ("Battery management controller", 350, 1, 1, 1), ("DC-DC converter", 300, 1, 1, 1), ("Display cluster", 260, 1, 1, 1),
        ("Body control module", 180, 2, 1, 1), ("Telematics control unit", 140, 1, 1, 1), ("Gateway module", 120, 2, 1, 1),
        ("Front camera module", 85, 3, 1, 2), ("Corner radar sensor", 70, 3, 2, 4), ("Seat control module", 45, 4, 2, 2),
        ("Key fob", 35, 2, 2, 2), ("Steering angle sensor", 30, 3, 1, 1), ("Window switch pack", 18, 6, 2, 4),
        ("Parking sensor", 14, 8, 4, 8), ("Ambient light module", 9, 10, 4, 12), ("Temperature sensor", 4, 15, 2, 8),
        ("Relay", 3, 15, 2, 10)]),
    "Wiring & harnesses": ("WIR", ["", "LH", "RH", "Front", "Rear"], [
        ("Main body harness", 450, 1, 1, 1), ("Instrument panel harness", 220, 1, 1, 1), ("HV cable assembly", 180, 3, 1, 3),
        ("Front end harness", 120, 1, 1, 1), ("Charge port harness", 110, 1, 1, 1), ("Motor phase cable", 90, 2, 3, 3),
        ("Door harness", 65, 4, 1, 2), ("Roof harness", 45, 2, 1, 1), ("Tailgate harness", 40, 2, 1, 1),
        ("Seat harness", 30, 3, 1, 2), ("Battery sense lead", 12, 6, 2, 6), ("Ground strap", 6, 10, 4, 10),
        ("Connector housing", 1.2, 25, 4, 20), ("Crimp terminal", 0.08, 25, 20, 80)]),
    "Body & chassis": ("BDY", ["", "LH", "RH", "Front", "Rear"], [
        ("Liftgate assembly", 450, 1, 1, 1), ("Rear subframe", 380, 1, 1, 1), ("Headlamp assembly", 320, 2, 1, 1),
        ("Windshield", 280, 1, 1, 1), ("Floor pan", 210, 1, 1, 1), ("Seat frame", 160, 3, 1, 2),
        ("Hood outer panel", 140, 1, 1, 1), ("Steering knuckle", 110, 2, 2, 2), ("Front bumper beam", 95, 1, 1, 1),
        ("Door inner panel", 85, 4, 1, 2), ("Lower control arm", 75, 3, 2, 2), ("Shock tower", 70, 2, 2, 2),
        ("Roof rail", 60, 2, 2, 2), ("Skid plate", 55, 2, 1, 1), ("Rocker reinforcement", 45, 3, 2, 2),
        ("Sway bar link", 18, 4, 2, 2), ("Fender liner", 12, 5, 2, 2), ("Tow hook bracket", 9, 4, 2, 2),
        ("Stamped reinforcement", 6, 20, 1, 4), ("Bracket", 3, 25, 2, 8)]),
    "Fasteners & fittings": ("FST", ["M5x12", "M6x16", "M8x20", "M8x25", "M10x30", "M12x40", "8mm", "10mm"], [
        ("Flange bolt", 0.18, 1, 4, 40), ("Hex nut", 0.06, 1, 4, 40), ("Trim clip", 0.05, 1, 6, 40),
        ("Blind rivet", 0.04, 1, 10, 40), ("Weld stud", 0.07, 1, 6, 30), ("Push pin", 0.03, 1, 6, 40),
        ("Torx screw", 0.09, 1, 4, 30), ("Flat washer", 0.02, 1, 4, 40), ("Spring nut", 0.11, 1, 4, 20),
        ("Hose clamp", 0.45, 1, 2, 12), ("Grommet", 0.25, 1, 2, 16), ("Cable tie mount", 0.08, 1, 4, 30)]),
}

part_ids = sorted(r.part_id for r in spark.table(f"{SCHEMA}.dim_part")
                  .filter(bucket("part_id", n=500, salt="bom") == 0).select("part_id").collect())
prng = np.random.default_rng(7)
cats = list(CATEGORY_MIX)
rows = []
for pid in part_ids:
    c = prng.choice(cats, p=[CATEGORY_MIX[k] for k in cats])
    prefix, suffixes, types = PART_TYPES[c]
    weights = np.array([t[2] for t in types], dtype=float)
    name, price, _, q_lo, q_hi = types[prng.choice(len(types), p=weights / weights.sum())]
    suffix = prng.choice(suffixes)
    rows.append((int(pid), f"{prefix}-{prng.integers(10000, 99999)}", f"{name} {suffix}".strip(), c,
                 float(price * prng.lognormal(0, 0.25)), int(prng.integers(q_lo, q_hi + 1))))

parts_pdf = pd.DataFrame(rows, columns=["part_id", "part_number", "part_name", "component_category", "raw_cost", "qty_per_vehicle"])
parts = spark.createDataFrame(parts_pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## bom
# MAGIC
# MAGIC Each part goes into about 2 of the 3 models, at the quantity drawn for its part type.

# COMMAND ----------

bom = save(
    parts.crossJoin(models.select("model_id"))
    .filter(bucket("part_id", "model_id", n=3, salt="bom_model") != 0)
    .select("model_id", "part_id", "qty_per_vehicle"),
    "dim_bom")

# COMMAND ----------

# MAGIC %md
# MAGIC ## standard cost
# MAGIC
# MAGIC One scale factor so an average vehicle's BOM comes to $45k. If it's far from 1, the typical prices above
# MAGIC are off and should be fixed rather than scaled.

# COMMAND ----------

VEHICLE_COST = 45_000
raw_vehicle_cost = (bom.join(parts.select("part_id", "raw_cost"), "part_id")
                    .agg(F.sum(F.col("qty_per_vehicle") * F.col("raw_cost")) / 3).first()[0])
scale = VEHICLE_COST / raw_vehicle_cost

catalog = save(
    parts.withColumn("standard_cost", F.round(F.col("raw_cost") * F.lit(scale), 4).cast("decimal(14,4)"))
    .select("part_id", "part_number", "part_name", "component_category", "standard_cost"),
    "dim_part_catalog")

# COMMAND ----------

import json
cost_by_model = (bom.join(catalog, "part_id").groupBy("model_id")
                 .agg(F.round(F.sum(F.col("qty_per_vehicle") * F.col("standard_cost")), 0).alias("c")).collect())
cost_by_cat = (bom.join(catalog, "part_id").groupBy("component_category")
               .agg(F.sum(F.col("qty_per_vehicle") * F.col("standard_cost")).alias("c")).collect())
result = {
    "price_scale_factor": round(scale, 3),
    "cost_share_by_category": {r.component_category: round(float(r.c) / 3 / VEHICLE_COST, 3) for r in cost_by_cat},
    "builds_rows": builds.count(),
    "parts": catalog.count(),
    "bom_rows": bom.count(),
    "bom_cost_per_vehicle": {r.model_id: float(r.c) for r in cost_by_model},
    "sample_parts": [f"{r.part_number} {r.part_name} ${float(r.standard_cost):,.2f}" for r in
                     catalog.orderBy(F.xxhash64("part_id")).limit(12).collect()],
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
