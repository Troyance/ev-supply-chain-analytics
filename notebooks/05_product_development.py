# Databricks notebook source
# MAGIC %md
# MAGIC # product development (plm)
# MAGIC
# MAGIC Stands in for a PLM system (think Teamcenter or Windchill). Writes to `ev_bronze`:
# MAGIC
# MAGIC - `plm_programs` - one vehicle program per production line, with its start of production (SOP)
# MAGIC - `plm_prototype_builds` - builds by phase: EP (engineering prototype), VP (validation prototype), PPV (pre-production)
# MAGIC - `plm_items` - item master: vehicles, systems, assemblies, parts, and parts that were replaced along the way
# MAGIC - `plm_bom_lines` - multi-level BOM (vehicle > system > assembly > part), every line effective-dated
# MAGIC - `plm_issues` - problems found in prototype builds and production, with severity and root cause
# MAGIC - `plm_engineering_changes` + `plm_eco_affected_items` - ECOs and the parts each one changes
# MAGIC
# MAGIC The story ties together: builds find issues, serious issues raise ECOs, implemented ECOs change BOM lines.
# MAGIC The current BOM explodes to exactly the flat BOM in `erp_bom`, so purchasing and engineering agree.
# MAGIC History is generated backwards from today's BOM: an ECO that replaced a part means an older line pointed
# MAGIC at a now-obsolete part until the ECO's implementation date.

# COMMAND ----------

import json
import numpy as np
import pandas as pd

BRONZE = "workspace.ev_bronze"
SNAPSHOT = pd.Timestamp("2026-11-21")
rng = np.random.default_rng(11)

def save(pdf, name):
    spark.createDataFrame(pdf).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{BRONZE}.{name}")

parts = spark.table(f"{BRONZE}.erp_parts").toPandas().sort_values("part_id").reset_index(drop=True)
bom = spark.table(f"{BRONZE}.erp_bom").toPandas().sort_values(["model_id", "part_id"]).reset_index(drop=True)
cat_of = dict(zip(parts.part_id, parts.component_category))

def d(s):
    return pd.Timestamp(s)

def days(n):
    return pd.Timedelta(days=int(n))

# COMMAND ----------

# MAGIC %md
# MAGIC ## programs and prototype builds
# MAGIC
# MAGIC One program per production line. Phases run EP -> VP -> PPV -> SOP, roughly 18 months end to end.

# COMMAND ----------

# program, vehicle model, plant, EP start, SOP (matches the production ramp in 03_production)
PROGRAMS = [
    ("PRG-PU1", "PU", "PLT1", "Pickup launch", "2020-03-02", "2021-09-13"),
    ("PRG-SUV1", "SUV", "PLT1", "SUV launch", "2020-05-04", "2021-12-06"),
    ("PRG-VAN1", "VAN", "PLT1", "Delivery van launch", "2020-08-03", "2022-03-07"),
    ("PRG-SUV2", "SUV", "PLT2", "SUV plant 2 launch", "2024-06-03", "2026-03-02"),
]
programs = pd.DataFrame(PROGRAMS, columns=["program_id", "model_id", "site_id", "program_name", "ep_start", "sop_date"])
programs["ep_start"] = pd.to_datetime(programs.ep_start).dt.date
programs["sop_date"] = pd.to_datetime(programs.sop_date).dt.date

# (phase, builds, share of the ep->sop window where it starts, vehicles per build, issues per vehicle)
PHASES = [("EP", 2, 0.00, (15, 30), 3.0), ("VP", 2, 0.40, (40, 80), 1.2), ("PPV", 1, 0.75, (120, 200), 0.35)]
builds = []
for p in programs.itertuples():
    span = (pd.Timestamp(p.sop_date) - pd.Timestamp(p.ep_start)).days
    for phase, n, start_share, (lo, hi), _ in PHASES:
        for i in range(n):
            planned = pd.Timestamp(p.ep_start) + days(span * start_share + i * 45)
            slip = int(rng.integers(0, 22))                  # builds usually slip a little
            builds.append((f"{p.program_id}-{phase}{i + 1}", p.program_id, phase, i + 1, planned.date(),
                           (planned + days(slip)).date(), int(rng.integers(lo, hi + 1))))
builds = pd.DataFrame(builds, columns=["build_id", "program_id", "build_phase", "build_number", "planned_start_date",
                                       "actual_start_date", "vehicles_built"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## item master and the multi-level BOM
# MAGIC
# MAGIC Systems come from the component category. Each vehicle model gets 4-7 assemblies per system, and every part
# MAGIC in that model's flat BOM hangs under one of them at the same quantity, so exploding the tree gives back the
# MAGIC flat BOM exactly.

# COMMAND ----------

SYSTEMS = {"Battery materials": ("SYS-BAT", "Battery pack"), "Electronics": ("SYS-ELE", "Electrical and ADAS"),
           "Wiring & harnesses": ("SYS-WIR", "Harnesses"), "Body & chassis": ("SYS-BDY", "Body and chassis"),
           "Fasteners & fittings": ("SYS-FST", "Joining and fittings")}
ASSEMBLY_WORDS = {"SYS-BAT": ["Module stack", "Thermal system", "Pack enclosure", "HV distribution", "Cell carrier", "BMS assembly", "Cooling loop"],
                  "SYS-ELE": ["ADAS sensor suite", "Infotainment", "Power electronics", "Body electronics", "Charging", "Gateway", "Lighting control"],
                  "SYS-WIR": ["Body harness set", "HV harness set", "Door harness set", "IP harness set", "Rear harness set", "Roof harness set", "Seat harness set"],
                  "SYS-BDY": ["Front structure", "Rear structure", "Closures", "Suspension", "Underbody", "Glazing", "Seating"],
                  "SYS-FST": ["Body fastener kit", "Chassis fastener kit", "Trim fastener kit", "Battery fastener kit", "Harness clip kit", "Fluid fittings", "Seal kit"]}

items, lines = [], []
model_names = {"PU": "Pickup", "SUV": "SUV", "VAN": "Delivery Van"}
vehicle_item = {}
# generated items get their own id range so they can never collide with real part ids (which go up to 1,000,000)
next_id = 10_000_001
for m in ["PU", "SUV", "VAN"]:
    vehicle_item[m] = next_id
    items.append((next_id, f"VEH-{m}", f"{model_names[m]} vehicle", "vehicle", None, "Released"))
    next_id += 1

assembly_of = {}                        # (model, part) -> assembly item id
program_start = {m: min(pd.Timestamp(p.ep_start) for p in programs.itertuples() if p.model_id == m) for m in ["PU", "SUV", "VAN"]}
for m in ["PU", "SUV", "VAN"]:
    start = program_start[m].date()
    for cat, (sys_code, sys_name) in SYSTEMS.items():
        sys_id = next_id; next_id += 1
        items.append((sys_id, f"{sys_code}-{m}", f"{sys_name} ({m})", "system", None, "Released"))
        lines.append((vehicle_item[m], sys_id, 1, start, None, None))
        n_asm = int(rng.integers(4, 8))
        asm_ids = []
        for k in range(n_asm):
            asm_id = next_id; next_id += 1
            asm_ids.append(asm_id)
            items.append((asm_id, f"ASM-{sys_code[4:]}-{m}-{k + 1:02d}", f"{ASSEMBLY_WORDS[sys_code][k]} ({m})", "assembly", None, "Released"))
            lines.append((sys_id, asm_id, 1, start, None, None))
        model_parts = bom[(bom.model_id == m) & (bom.part_id.map(cat_of) == cat)]
        for r in model_parts.itertuples():
            asm = asm_ids[int(rng.integers(0, n_asm))]
            assembly_of[(m, int(r.part_id))] = asm
            lines.append((asm, int(r.part_id), int(r.qty_per_vehicle), start, None, None))

for r in parts.itertuples():
    items.append((int(r.part_id), r.part_number, r.part_name, "part", r.component_category, "Released"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## issues
# MAGIC
# MAGIC Prototype builds find a lot (EP worst, PPV least), then production keeps finding a trickle. Electronics and
# MAGIC battery find more than fasteners. Severity S1 is a stop-ship, S4 is cosmetic; serious ones close faster.

# COMMAND ----------

SYS_WEIGHT = {"Battery materials": 0.24, "Electronics": 0.32, "Wiring & harnesses": 0.16, "Body & chassis": 0.22, "Fasteners & fittings": 0.06}
SEVERITY = [("S1", 0.06, 9), ("S2", 0.22, 18), ("S3", 0.42, 30), ("S4", 0.30, 45)]       # (code, share, typical days to close)
ROOT_CAUSE = [("Design", 0.38), ("Supplier quality", 0.24), ("Assembly process", 0.18), ("Software", 0.14), ("Test or measurement", 0.06)]
cats = list(SYS_WEIGHT)

def pick(options, weights):
    w = np.array(weights, dtype=float)
    return options[int(rng.choice(len(options), p=w / w.sum()))]

issues = []
def add_issue(program, model, build_id, phase, found):
    if found > SNAPSHOT:                             # nothing gets found in the future
        return
    cat = pick(cats, [SYS_WEIGHT[c] for c in cats])
    pool = bom[(bom.model_id == model) & (bom.part_id.map(cat_of) == cat)].part_id.to_numpy()
    part = int(pool[int(rng.integers(0, len(pool)))])
    sev, _, typical = SEVERITY[int(rng.choice(4, p=[s[1] for s in SEVERITY]))]
    closed = found + days(max(1, rng.lognormal(np.log(typical), 0.6)))
    issues.append([None, program, build_id, phase, part, cat, sev, pick([c for c, _ in ROOT_CAUSE], [w for _, w in ROOT_CAUSE]),
                   found.date(), closed.date() if closed <= SNAPSHOT else None])

for b in builds.itertuples():
    model = programs.set_index("program_id").at[b.program_id, "model_id"]
    rate = dict((p[0], p[4]) for p in PHASES)[b.build_phase]
    for _ in range(int(rng.poisson(b.vehicles_built * rate))):
        add_issue(b.program_id, model, b.build_id, b.build_phase, pd.Timestamp(b.actual_start_date) + days(rng.integers(0, 60)))

for p in programs.itertuples():                     # production issues: about 6 a month, tailing off
    month = pd.Timestamp(p.sop_date)
    while month < SNAPSHOT:
        age = (month - pd.Timestamp(p.sop_date)).days / 365
        for _ in range(int(rng.poisson(9 * np.exp(-age / 1.5) + 2))):
            add_issue(p.program_id, p.model_id, None, "Production", month + days(rng.integers(0, 28)))
        month += pd.DateOffset(months=1)

issues = pd.DataFrame(issues, columns=["issue_id", "program_id", "build_id", "found_phase", "part_id", "component_category",
                                       "severity", "root_cause", "found_date", "closed_date"])
issues = issues.sort_values(["found_date", "program_id", "part_id"]).reset_index(drop=True)
issues["issue_id"] = [f"ISS-{i + 10001}" for i in range(len(issues))]

# COMMAND ----------

# MAGIC %md
# MAGIC ## engineering changes
# MAGIC
# MAGIC Serious issues raise ECOs (half of S1/S2, a sixth of S3). Other ECOs come from cost reduction, supplier
# MAGIC changes and regulation. Each moves draft > submitted > reviewed > approved > implemented. Implementation
# MAGIC waits on tooling, so body and battery changes take longest. Changes raised close to SOP get rushed through
# MAGIC review but still wait on tooling.

# COMMAND ----------

IMPL_DAYS = {"Battery materials": 55, "Electronics": 28, "Wiring & harnesses": 24, "Body & chassis": 62, "Fasteners & fittings": 12}
ecos, affected = [], []
used = set()                                         # each (model, part) is changed at most once, keeps history simple

def raise_eco(program, model, created, reason, cat, issue_id, priority):
    if created > SNAPSHOT:                           # nothing gets raised in the future
        return
    pool = [int(x) for x in bom[(bom.model_id == model) & (bom.part_id.map(cat_of) == cat)].part_id if (model, int(x)) not in used]
    if not pool:
        return
    sop = pd.Timestamp(programs.set_index("program_id").at[program, "sop_date"])
    rush = 0.6 if abs((created - sop).days) < 120 else 1.0
    t = {"created": created}
    t["submitted"] = t["created"] + days(rng.integers(1, 6))
    t["reviewed"] = t["submitted"] + days(max(1, rng.lognormal(np.log(8 * rush), 0.5)))
    t["approved"] = t["reviewed"] + days(max(1, rng.lognormal(np.log(5 * rush), 0.5)))
    t["implemented"] = t["approved"] + days(max(3, rng.lognormal(np.log(IMPL_DAYS[cat]), 0.45)))
    rejected = reason != "Quality issue" and rng.random() < 0.08
    stamp = {k: (v.date() if v <= SNAPSHOT else None) for k, v in t.items()}
    if rejected:
        stamp["approved"] = stamp["implemented"] = None
    for later, earlier in [("reviewed", "submitted"), ("approved", "reviewed"), ("implemented", "approved")]:
        if stamp[earlier] is None:
            stamp[later] = None
    status = ("Rejected" if rejected and stamp["reviewed"] else
              "Implemented" if stamp["implemented"] else "Approved" if stamp["approved"] else
              "In review" if stamp["submitted"] else "Draft")
    eco_id = f"ECO-{len(ecos) + 5001}"
    ecos.append((eco_id, program, reason, priority, cat, issue_id, status, stamp["created"], stamp["submitted"],
                 stamp["reviewed"], stamp["approved"], stamp["implemented"]))
    n = min(len(pool), int(rng.integers(1, 4)))
    for part in rng.choice(pool, size=n, replace=False):
        used.add((model, int(part)))
        change = pick(["Replace part", "Quantity change", "Add part", "Remove part"], [0.55, 0.2, 0.15, 0.1])
        affected.append((eco_id, model, int(part), change))

model_of = programs.set_index("program_id").model_id.to_dict()
for r in issues.itertuples():
    p_eco = {"S1": 0.5, "S2": 0.5, "S3": 0.16}.get(r.severity, 0)
    if rng.random() < p_eco:
        raise_eco(r.program_id, model_of[r.program_id], pd.Timestamp(r.found_date) + days(rng.integers(2, 15)),
                  "Quality issue", r.component_category, r.issue_id, "High" if r.severity == "S1" else "Medium")
for p in programs.itertuples():
    month = pd.Timestamp(p.ep_start)
    while month < SNAPSHOT:
        for _ in range(int(rng.poisson(2.5))):
            reason = pick(["Cost reduction", "Supplier change", "Design improvement", "Regulatory"], [0.35, 0.25, 0.3, 0.1])
            raise_eco(p.program_id, p.model_id, month + days(rng.integers(0, 28)), reason,
                      pick(cats, [SYS_WEIGHT[c] for c in cats]), None, "Low" if reason == "Cost reduction" else "Medium")
        month += pd.DateOffset(months=1)

ecos = pd.DataFrame(ecos, columns=["eco_id", "program_id", "reason", "priority", "component_category", "issue_id", "status",
                                   "created_date", "submitted_date", "reviewed_date", "approved_date", "implemented_date"])
affected = pd.DataFrame(affected, columns=["eco_id", "model_id", "part_id", "change_type"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## BOM history from implemented ECOs
# MAGIC
# MAGIC Worked backwards from today's BOM. For an implemented ECO effective on date X:
# MAGIC
# MAGIC - replace: the current line starts on X, and an older line pointed at an obsolete part until X - 1
# MAGIC - quantity change: same part, a different quantity until X - 1
# MAGIC - add: the line simply starts on X
# MAGIC - remove: an obsolete part's line ends on X - 1, and there's no current line
# MAGIC
# MAGIC ECOs that aren't implemented yet don't touch the BOM.

# COMMAND ----------

lines = pd.DataFrame(lines, columns=["parent_item_id", "child_item_id", "qty", "effective_from", "effective_to", "eco_id"])
impl = ecos.set_index("eco_id").implemented_date.to_dict()
obsolete_id = 20_000_001
extra_items, extra_lines = [], []
for a in affected.itertuples():
    eff = impl.get(a.eco_id)
    if eff is None:
        continue
    asm = assembly_of[(a.model_id, a.part_id)]
    mask = (lines.parent_item_id == asm) & (lines.child_item_id == a.part_id)
    start = lines.loc[mask, "effective_from"].iloc[0]
    if eff <= start:
        continue
    before = eff - pd.Timedelta(days=1)
    pn = parts.set_index("part_id").loc[a.part_id]
    if a.change_type in ("Replace part", "Remove part"):
        extra_items.append((obsolete_id, f"{pn.part_number}-OLD", f"{pn.part_name} (superseded)", "part", pn.component_category, "Obsolete"))
        extra_lines.append((asm, obsolete_id, int(lines.loc[mask, "qty"].iloc[0]), start, before, None))
        obsolete_id += 1
        if a.change_type == "Remove part":
            # the old part was removed: the current part's line is new, introduced by this eco
            lines.loc[mask, ["effective_from", "eco_id"]] = [eff, a.eco_id]
            continue
    elif a.change_type == "Quantity change":
        old_qty = max(1, int(lines.loc[mask, "qty"].iloc[0]) + int(rng.choice([-2, -1, 1, 2])))
        extra_lines.append((asm, a.part_id, old_qty, start, before, None))
    lines.loc[mask, ["effective_from", "eco_id"]] = [eff, a.eco_id]

items = pd.DataFrame(items + extra_items, columns=["item_id", "item_number", "item_name", "item_type", "component_category", "lifecycle_status"])
lines = pd.concat([lines, pd.DataFrame(extra_lines, columns=lines.columns)], ignore_index=True)
lines["effective_to"] = lines.effective_to.where(lines.effective_to.notna(), None)
lines["eco_id"] = lines.eco_id.where(lines.eco_id.notna(), None)
lines = lines.sort_values(["parent_item_id", "child_item_id", "effective_from"]).reset_index(drop=True)

# COMMAND ----------

for df, cols in [(issues, ["build_id", "closed_date"]), (ecos, ["issue_id", "submitted_date", "reviewed_date", "approved_date", "implemented_date"]),
                 (items, ["component_category"])]:
    for c in cols:
        df[c] = df[c].astype(object).where(df[c].notna(), None)

save(programs, "plm_programs")
save(builds, "plm_prototype_builds")
save(items, "plm_items")
spark.createDataFrame(lines, "parent_item_id bigint, child_item_id bigint, qty int, effective_from date, effective_to date, eco_id string") \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{BRONZE}.plm_bom_lines")
spark.createDataFrame(issues, "issue_id string, program_id string, build_id string, found_phase string, part_id bigint, component_category string, severity string, root_cause string, found_date date, closed_date date") \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{BRONZE}.plm_issues")
spark.createDataFrame(ecos, "eco_id string, program_id string, reason string, priority string, component_category string, issue_id string, status string, created_date date, submitted_date date, reviewed_date date, approved_date date, implemented_date date") \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{BRONZE}.plm_engineering_changes")
save(affected, "plm_eco_affected_items")

result = {"programs": len(programs), "builds": len(builds), "items": len(items), "bom_lines": len(lines),
          "historic_bom_lines": int(lines.effective_to.notna().sum()), "issues": len(issues),
          "issues_by_phase": issues.found_phase.value_counts().to_dict(), "ecos": len(ecos),
          "ecos_by_status": ecos.status.value_counts().to_dict(), "affected_items": len(affected)}
print(json.dumps(result, indent=2, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
