"""builds dashboards/supply_chain.json - the ai/bi dashboard. everything reads ev_gold.
metric views are used directly as datasets, so widgets call MEASURE(`On-Time Rate`) instead of re-implementing it.
run: python3 dashboards/build_dashboard.py, then databricks bundle deploy"""
import json, pathlib

GOLD = "workspace.ev_gold"

def sql(*lines):
    return [l.rstrip() + "\n" for l in lines]

def metric_view(name, view):
    return {"name": name, "displayName": view, "asset_name": f"{GOLD}.{view}"}

datasets = [
    metric_view("mv_delivery", "supplier_delivery_metrics"),
    metric_view("mv_risk", "sourcing_risk_metrics"),
    metric_view("mv_planning", "inventory_planning_metrics"),
    metric_view("mv_accuracy", "forecast_accuracy_metrics"),
    metric_view("mv_policy", "policy_simulation_metrics"),
    metric_view("mv_eco", "engineering_change_metrics"),
    metric_view("mv_issues", "prototype_issue_metrics"),
    {"name": "ds_burndown", "displayName": "Issue burndown",
     "queryLines": sql("SELECT week_start, program_name, severity, issues_open FROM mart_issue_burndown")},
    {"name": "ds_churn", "displayName": "BOM churn",
     "queryLines": sql("SELECT program_name, phase, phase_order, bom_churn_rate FROM mart_bom_churn")},
    {"name": "ds_exposure", "displayName": "Engineering change exposure",
     "queryLines": sql("SELECT site_id, part_number, part_name, eco_id, eco_status, change_type, signal, on_hand_qty,",
                       "       suggested_order_value, stock_at_risk_value, recommended_action",
                       "FROM mart_eco_supply_exposure")},
    {"name": "ds_scorecard", "displayName": "Supplier scorecard",
     "queryLines": sql("SELECT supplier_name, country, region, tier, spend, on_time_rate, on_time_rate_prior_12m,",
                       "       on_time_rate_last_12m, lateness_trend_days_per_month, is_declining",
                       "FROM mart_supplier_scorecard")},
    {"name": "ds_risk_top", "displayName": "Biggest high-risk parts",
     "queryLines": sql("SELECT part_number, part_name, component_category, top_supplier_name, top_supplier_country,",
                       "       top_supplier_region, top_supplier_tier, total_spend",
                       "FROM mart_sourcing_risk WHERE is_high_risk ORDER BY total_spend DESC LIMIT 200")},
    {"name": "ds_builds", "displayName": "Build plan",
     "queryLines": sql("SELECT week_start, production_line, actual_units, forecast_p50",
                       "FROM mart_vehicle_build_plan WHERE week_start >= '2024-01-01'")},
    {"name": "ds_signals", "displayName": "Buy signals",
     "queryLines": sql("SELECT site_id, part_number, part_name, component_category, buffer_class, supplier_name,",
                       "       supplier_region, supplier_tier, daily_run_rate, days_of_cover, planning_lead_time_days,",
                       "       safety_stock_days, suggested_order_qty, suggested_order_value, signal,",
                       "       CASE signal WHEN 'EXPEDITE' THEN 1 WHEN 'ORDER' THEN 2 WHEN 'EXCESS' THEN 3 ELSE 4 END AS signal_rank",
                       "FROM mart_buy_signals")},
    {"name": "ds_sawtooth", "displayName": "Policy sawtooth",
     "queryLines": sql("SELECT part_label, sim_date, policy_name, on_hand_units FROM mart_policy_daily")},
    {"name": "ds_projection", "displayName": "13-week projection",
     "queryLines": sql("SELECT part_label, projection_date, on_hand_units, reorder_point_units, safety_stock_units",
                       "FROM mart_inventory_projection_daily")},
]

# ---------- helpers ----------
def f(name, expr):
    return {"name": name, "expression": expr}

def m(measure):
    """a metric view measure as a widget field"""
    return f(f"measure({measure})", f"MEASURE(`{measure}`)")

def dim(col):
    return f(col, f"`{col}`")

def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}

def text(name, md, p):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [md]}}, "position": p}

USD = {"type": "number-currency", "currencyCode": "USD", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}
NUM = {"type": "number-plain", "decimalPlaces": {"type": "max", "places": 1}}
BAD, WARN, GOOD, MUTED = "#E15759", "#F28E2C", "#59A14F", "#9AA5B1"

def widget(name, ds, fields, spec, p, disagg=False, orders=None):
    q = {"datasetName": ds, "fields": fields, "disaggregated": disagg}
    if orders:
        q["orders"] = orders
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": q}], "spec": spec}, "position": p}

def counter(name, ds, measure, title, p, fmt=None):
    value = {"fieldName": f"measure({measure})", "displayName": title}
    if fmt:
        value["format"] = fmt
    return widget(name, ds, [m(measure)],
                  {"version": 2, "widgetType": "counter", "encodings": {"value": value}, "frame": {"title": title, "showTitle": True}}, p)

def chart(name, kind, ds, x, ys, title, p, color=None, mappings=None, x_type="categorical", fmt=None, desc=None,
          extra=None, x_name=None):
    """x: column name, or (field_name, expression) for a derived one. ys: list of (field, display)"""
    x_field = f(*x) if isinstance(x, tuple) else dim(x)
    fields = [x_field] + [y[0] for y in ys] + ([dim(color)] if color else [])
    y_enc = {"scale": {"type": "quantitative"}}
    if len(ys) == 1:
        y_enc.update({"fieldName": ys[0][0]["name"], "displayName": ys[0][1]})
    else:
        y_enc["fields"] = [{"fieldName": y[0]["name"], "displayName": y[1]} for y in ys]
    if fmt:
        y_enc["format"] = fmt
    enc = {"x": {"fieldName": x_field["name"], "scale": {"type": x_type}, "displayName": x_name or x_field["name"]}, "y": y_enc}
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical", **({"mappings": mappings} if mappings else {})},
                        "displayName": color}
    spec = {"version": 3, "widgetType": kind, "encodings": enc, "frame": {"title": title, "showTitle": True}}
    if desc:
        spec["frame"].update({"showDescription": True, "description": desc})
    if extra:
        spec.update(extra)
    return widget(name, ds, fields, spec, p)

def table(name, ds, cols, title, p, orders=None):
    columns = [{"fieldName": c[0], "displayName": c[1], **({"format": c[2]} if len(c) > 2 and c[2] else {})} for c in cols]
    return widget(name, ds, [dim(c[0]) for c in cols],
                  {"version": 2, "widgetType": "table", "encodings": {"columns": columns}, "frame": {"title": title, "showTitle": True}},
                  p, disagg=True, orders=orders)

def page(name, display, layout):
    return {"name": name, "displayName": display, "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": layout}

def marker(date, label, color):
    return {"type": "vertical-line", "encodings": {"x": {"dataValue": f"{date}T00:00:00.000", "dataType": "DATETIME"},
                                                    "label": {"value": label}, "color": {"value": color}}}

def filt(name, kind, title, bindings, p, default=None):
    """bindings: list of (dataset, field) - one filter can drive datasets whose column names differ"""
    queries = [{"name": f"{name}_{i}", "query": {"datasetName": ds, "fields": [dim(col)], "disaggregated": False}}
               for i, (ds, col) in enumerate(bindings)]
    spec = {"version": 2, "widgetType": kind, "frame": {"showTitle": True, "title": title},
            "encodings": {"fields": [{"fieldName": col, "displayName": title, "queryName": q["name"]}
                                     for q, (ds, col) in zip(queries, bindings)]}}
    if default:
        spec["selection"] = {"defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": default}]}}}
    return {"widget": {"name": name, "queries": queries, "spec": spec}, "position": p}

TIER_COLORS = [{"value": "A", "color": GOOD}, {"value": "B", "color": WARN}, {"value": "C", "color": BAD}, {"value": "Unrated", "color": MUTED}]
SIGNAL_COLORS = [{"value": "EXPEDITE", "color": BAD}, {"value": "ORDER", "color": WARN}, {"value": "OK", "color": GOOD},
                 {"value": "EXCESS", "color": MUTED}]
POLICY_COLORS = [{"value": "Current", "color": "#4E79A7"}, {"value": "Lean", "color": BAD}, {"value": "High service", "color": GOOD},
                 {"value": "EOQ", "color": "#B07AA1"}, {"value": "DDMRP", "color": WARN}, {"value": "MRP (schedule)", "color": "#76B7B2"}]
MODEL_COLORS = [{"value": "LightGBM", "color": GOOD}, {"value": "Naive baseline", "color": MUTED}]

# ---------- page 1: suppliers ----------
suppliers = [
    text("title-suppliers", "# Supplier performance\nHow reliable the supplier base is, where it's slow, and who is getting worse. Asia takes "
         "about 4x longer than north america. Electronics lead times and lateness spiked during the 2021-22 chip shortage.", pos(0, 0, 12, 2)),
    counter("kpi-spend", "mv_delivery", "Spend", "Spend (all time)", pos(0, 2, 3, 3), USD),
    counter("kpi-otd", "mv_delivery", "On-Time Rate", "On-time delivery", pos(3, 2, 3, 3), PCT),
    counter("kpi-suppliers", "mv_delivery", "Active Suppliers", "Active suppliers", pos(6, 2, 3, 3)),
    counter("kpi-lead", "mv_delivery", "Avg Lead Time Days", "Avg lead time (days)", pos(9, 2, 3, 3), NUM),
    chart("lead-trend", "line", "mv_delivery", ("monthly(Order Month)", 'DATE_TRUNC("MONTH", `Order Month`)'),
          [(m("Avg Lead Time Days"), "Avg lead time (days)")], "Lead time by component category (order month)", pos(0, 5, 8, 6),
          color="Component Category", x_type="temporal", fmt=NUM, x_name="Order month",
          extra={"annotations": [marker("2021-06-01", "Chip shortage starts", BAD), marker("2022-04-01", "Worst point", BAD),
                                 marker("2022-12-31", "Shortage over", GOOD)]}),
    chart("lead-region", "bar", "mv_delivery", "Supplier Region", [(m("Avg Lead Time Days"), "Avg lead time (days)")],
          "Lead time by supplier region", pos(8, 5, 4, 6), fmt=NUM),
    chart("otd-trend", "line", "mv_delivery", ("monthly(Receipt Month)", 'DATE_TRUNC("MONTH", `Receipt Month`)'),
          [(m("On-Time Rate"), "On-time delivery")], "On-time delivery by region", pos(0, 11, 6, 5),
          color="Supplier Region", x_type="temporal", fmt=PCT, x_name="Receipt month"),
    chart("spend-tier", "bar", "mv_delivery", "Supplier Region", [(m("Spend"), "Spend")],
          "Spend by region and supplier tier at the time of delivery", pos(6, 11, 6, 5), color="Supplier Tier",
          mappings=TIER_COLORS, fmt=USD),
    table("declining", "ds_scorecard", [
        ("supplier_name", "Supplier"), ("country", "Country"), ("tier", "Tier"),
        ("lateness_trend_days_per_month", "Days later / month", NUM), ("on_time_rate_prior_12m", "On-time (prior 12m)", PCT),
        ("on_time_rate_last_12m", "On-time (last 12m)", PCT), ("spend", "Spend", USD)],
        "Declining suppliers (getting later every month)", pos(0, 16, 12, 6),
        orders=[{"direction": "DESC", "expression": "`lateness_trend_days_per_month`"}]),
]
suppliers[-1]["widget"]["queries"][0]["query"]["filters"] = [{"expression": "`is_declining`"}]

# ---------- page 2: sourcing risk ----------
risk = [
    text("title-risk", "# Sourcing risk\nMost auto parts are single-sourced on purpose - tooling is expensive. The risk is depending on "
         "a single supplier who isn't reliable: high risk = single-sourced from a tier C supplier.", pos(0, 0, 12, 2)),
    counter("kpi-single", "mv_risk", "Single-Sourced Parts", "Single-sourced parts", pos(0, 2, 4, 3)),
    counter("kpi-high", "mv_risk", "High-Risk Parts", "High-risk parts", pos(4, 2, 4, 3)),
    counter("kpi-at-risk", "mv_risk", "Spend at Risk", "Spend at risk", pos(8, 2, 4, 3), USD),
    chart("risk-cat", "bar", "mv_risk", "Component Category", [(m("Spend at Risk"), "Spend at risk")],
          "Spend at risk by category", pos(0, 5, 6, 6), color="Supplier Region", fmt=USD),
    chart("risk-tier", "bar", "mv_risk", "Supplier Tier", [(m("Single-Sourced Parts"), "Single-sourced parts")],
          "Single-sourced parts by supplier tier", pos(6, 5, 6, 6), color="Supplier Tier", mappings=TIER_COLORS),
    table("risk-top", "ds_risk_top", [
        ("part_number", "Part #"), ("part_name", "Part"), ("component_category", "Category"), ("top_supplier_name", "Supplier"),
        ("top_supplier_country", "Country"), ("top_supplier_tier", "Tier"), ("total_spend", "Spend", USD)],
        "Biggest high-risk parts", pos(0, 11, 12, 7)),
]

# ---------- page 3: forecast and buy signals ----------
planning = [
    text("title-planning", "# Forecast & buy signals\nVehicle builds are forecast 13 weeks out (LightGBM), exploded through the BOM into "
         "part demand, then compared with stock to decide what to order. Plant 2 is still ramping.", pos(0, 0, 12, 2)),
    counter("kpi-expedite", "mv_planning", "Parts to Expedite", "Parts to expedite", pos(0, 2, 3, 3)),
    counter("kpi-order", "mv_planning", "Parts to Order", "Parts to order", pos(3, 2, 3, 3)),
    counter("kpi-order-value", "mv_planning", "Suggested Order Value", "Suggested order value", pos(6, 2, 3, 3), USD),
    counter("kpi-cover", "mv_planning", "Days of Cover", "Days of cover", pos(9, 2, 3, 3), NUM),
    chart("builds", "line", "ds_builds", ("weekly(week_start)", 'DATE_TRUNC("WEEK", `week_start`)'),
          [(f("sum(actual_units)", "SUM(`actual_units`)"), "Actual"), (f("sum(forecast_p50)", "SUM(`forecast_p50`)"), "Forecast")],
          "Weekly builds: actual and forecast", pos(0, 5, 8, 7), x_type="temporal", x_name="Week",
          extra={"facet": {"type": "wrap", "fieldName": "production_line", "scale": {"type": "categorical"},
                           "layout": {"column": {"type": "count", "value": 2}}}}),
    chart("accuracy", "line", "mv_accuracy", "Horizon Weeks", [(m("WAPE"), "WAPE")], "Forecast error by weeks ahead (backtest)",
          pos(8, 5, 4, 7), color="Model", mappings=MODEL_COLORS, x_type="quantitative", fmt=PCT,
          desc="Lower is better. LightGBM vs a flat recent-average baseline."),
    chart("signals-class", "bar", "mv_planning", "Buffer Class", [(m("Parts"), "Parts")], "Buy signals by buffer class",
          pos(0, 12, 12, 5), color="Signal", mappings=SIGNAL_COLORS),
    table("signals", "ds_signals", [
        ("signal", "Signal"), ("site_id", "Site"), ("part_number", "Part #"), ("part_name", "Part"), ("buffer_class", "Buffer"),
        ("supplier_name", "Supplier"), ("daily_run_rate", "DRR", NUM), ("days_of_cover", "Days of cover", NUM),
        ("planning_lead_time_days", "Lead time (d)"), ("safety_stock_days", "Safety stock (d)", NUM),
        ("suggested_order_qty", "Order qty"), ("suggested_order_value", "Order value", USD)],
        "Buy signals, most urgent first", pos(0, 17, 12, 8),
        orders=[{"direction": "ASC", "expression": "`signal_rank`"}, {"direction": "DESC", "expression": "`suggested_order_value`"}]),
]
planning[5]["widget"]["queries"][0]["query"]["fields"].append(dim("production_line"))   # the facet needs its field

# ---------- page 4: ordering policies ----------
DEFAULT_PART = "PLT1 BDY-79568 Fender liner"
policies = [
    text("title-policies", "# Ordering policies\nThe last 52 weeks of real demand replayed under six ordering policies - same demand, "
         "same late deliveries, same starting point. **Stockout rate** is the share of parts that ran out at least once; unit fill rate "
         "always looks high because cheap high-volume parts dominate it. Plant 2 is ramping, and only schedule-driven MRP keeps up.",
         pos(0, 0, 12, 2)),
    filt("filter-part", "filter-single-select", "Part (for the sawtooth charts)",
         [("ds_sawtooth", "part_label"), ("ds_projection", "part_label")], pos(0, 2, 12, 2), default=DEFAULT_PART),
    chart("policy-cost", "bar", "mv_policy", "Policy",
          [(m("Holding Cost"), "Holding"), (m("Ordering Cost"), "Ordering"), (m("Shortage Cost"), "Shortage")],
          "Annual cost by policy", pos(0, 4, 6, 6), fmt=USD),
    chart("policy-stockout", "bar", "mv_policy", "Policy", [(m("Stockout Rate"), "Stockout rate")],
          "Share of parts that ran out at least once", pos(6, 4, 3, 6), color="Policy", mappings=POLICY_COLORS, fmt=PCT),
    chart("policy-cash", "bar", "mv_policy", "Policy", [(m("Avg Inventory Value"), "Avg inventory")],
          "Cash tied up in stock", pos(9, 4, 3, 6), color="Policy", mappings=POLICY_COLORS, fmt=USD),
    chart("policy-site", "bar", "mv_policy", "Site", [(m("Stockout Rate"), "Stockout rate")], "Stockout rate by site and policy",
          pos(0, 10, 6, 6), color="Policy", mappings=POLICY_COLORS, fmt=PCT, extra={"mark": {"layout": "group"}}),
    chart("policy-class", "bar", "mv_policy", "Buffer Class", [(m("Total Cost"), "Total cost")], "Total cost by buffer class and policy",
          pos(6, 10, 6, 6), color="Policy", mappings=POLICY_COLORS, fmt=USD, extra={"mark": {"layout": "group"}}),
    chart("sawtooth", "line", "ds_sawtooth", "sim_date", [(f("sum(on_hand_units)", "SUM(`on_hand_units`)"), "On hand (units)")],
          "Sawtooth: on-hand stock for the selected part under each policy", pos(0, 16, 12, 7),
          color="policy_name", mappings=POLICY_COLORS, x_type="temporal", x_name="Day"),
    chart("projection", "line", "ds_projection", "projection_date",
          [(f("sum(on_hand_units)", "SUM(`on_hand_units`)"), "On hand"),
           (f("sum(reorder_point_units)", "SUM(`reorder_point_units`)"), "Reorder point"),
           (f("sum(safety_stock_units)", "SUM(`safety_stock_units`)"), "Safety stock")],
          "Next 13 weeks from today's stock (current policy)", pos(0, 23, 12, 6), x_type="temporal", x_name="Day"),
]

# ---------- page 5: product development ----------
PHASES = {"sort": {"by": "custom-order", "orderedValues": ["EP", "VP", "PPV", "Production"]}}
SEV_COLORS = [{"value": "S1", "color": BAD}, {"value": "S2", "color": WARN}, {"value": "S3", "color": "#EDC948"}, {"value": "S4", "color": MUTED}]
product = [
    text("title-product", "# Product development\nPrototype builds find issues, serious issues raise engineering changes (ECOs), and implemented "
         "changes update the BOM. Issues fall as programs mature from EP to PPV, BOM churn drops as the design freezes, and pending "
         "changes flag parts that shouldn't be reordered.", pos(0, 0, 12, 2)),
    counter("kpi-ecos", "mv_eco", "Changes", "Engineering changes", pos(0, 2, 3, 3)),
    counter("kpi-open-ecos", "mv_eco", "Open Changes", "Open changes", pos(3, 2, 3, 3)),
    counter("kpi-cycle", "mv_eco", "Avg Cycle Time Days", "Avg change cycle time (days)", pos(6, 2, 3, 3), NUM),
    counter("kpi-issues", "mv_issues", "Issues", "Issues found", pos(9, 2, 3, 3)),
    chart("issues-phase", "bar", "mv_issues", "Found Phase", [(m("Issues"), "Issues")], "Issues by phase and severity",
          pos(0, 5, 6, 6), color="Severity", mappings=SEV_COLORS),
    chart("eco-stages", "bar", "mv_eco", "Raised In Phase",
          [(m("Avg Days In Review"), "Review"), (m("Avg Days To Approve"), "Approval"), (m("Avg Days To Implement"), "Implementation")],
          "Where engineering change time goes, by phase raised", pos(6, 5, 6, 6), fmt=NUM,
          desc="Average days per stage. Implementation is mostly tooling and supplier lead time."),
    chart("burndown", "line", "ds_burndown", ("weekly(week_start)", 'DATE_TRUNC("WEEK", `week_start`)'),
          [(f("sum(issues_open)", "SUM(`issues_open`)"), "Open issues")], "Open issues by program", pos(0, 11, 8, 6),
          color="program_name", x_type="temporal", x_name="Week"),
    chart("churn", "bar", "ds_churn", "phase", [(f("sum(bom_churn_rate)", "SUM(`bom_churn_rate`)"), "Share of BOM changed")],
          "BOM churn by phase", pos(8, 11, 4, 6), color="program_name", fmt=PCT, extra={"mark": {"layout": "group"}},
          desc="Share of each program's BOM changed in each phase. It falls as the design freezes for launch."),
    table("exposure", "ds_exposure", [
        ("site_id", "Site"), ("part_number", "Part #"), ("part_name", "Part"), ("eco_id", "ECO"), ("eco_status", "ECO status"),
        ("change_type", "Change"), ("signal", "Buy signal"), ("stock_at_risk_value", "Stock at risk", USD),
        ("recommended_action", "Action")],
        "Parts being superseded by a pending engineering change", pos(0, 17, 12, 7),
        orders=[{"direction": "DESC", "expression": "`stock_at_risk_value`"}]),
]
product[5]["widget"]["spec"]["encodings"]["x"]["scale"].update(PHASES)
product[6]["widget"]["spec"]["encodings"]["x"]["scale"].update(PHASES)
product[8]["widget"]["spec"]["encodings"]["x"]["scale"].update(PHASES)

# ---------- global filters ----------
filters = {"name": "filters", "displayName": "Filters", "pageType": "PAGE_TYPE_GLOBAL_FILTERS", "layoutVersion": "GRID_V1", "layout": [
    filt("filter-region", "filter-multi-select", "Supplier region",
         [("mv_delivery", "Supplier Region"), ("mv_risk", "Supplier Region"), ("mv_planning", "Supplier Region"),
          ("ds_scorecard", "region"), ("ds_risk_top", "top_supplier_region"), ("ds_signals", "supplier_region")], pos(0, 0, 4, 2)),
    filt("filter-category", "filter-multi-select", "Component category",
         [("mv_delivery", "Component Category"), ("mv_risk", "Component Category"), ("mv_planning", "Component Category"),
          ("mv_policy", "Component Category"), ("ds_risk_top", "component_category"), ("ds_signals", "component_category")],
         pos(4, 0, 4, 2)),
    filt("filter-site", "filter-multi-select", "Site",
         [("mv_planning", "Site"), ("mv_policy", "Site"), ("ds_signals", "site_id")], pos(8, 0, 4, 2)),
]}

dashboard = {
    "datasets": datasets,
    "pages": [page("suppliers", "Suppliers", suppliers), page("risk", "Sourcing risk", risk),
              page("planning", "Forecast & buy signals", planning), page("policies", "Ordering policies", policies),
              page("product", "Product development", product), filters],
    "uiSettings": {"theme": {
        "canvasBackgroundColor": {"light": "#F7F8FA", "dark": "#1B2128"},
        "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"},
        "widgetBorderColor": {"light": "#FFFFFF", "dark": "#11171C"},
        "fontColor": {"light": "#11171C", "dark": "#E8ECF0"},
        "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"},
        "visualizationColors": ["#4E79A7", "#F28E2C", "#76B7B2", "#B07AA1", "#EDC948", "#9C755F", "#59A14F"],
        "widgetHeaderAlignment": "LEFT"},
        # "ask genie" button on the dashboard, pointing at the EV Supply Chain Analyst agent (genie/build_genie.py)
        "genieSpace": {"isEnabled": True, "overrideId": (pathlib.Path(__file__).parent.parent / "genie" / ".space_id").read_text().strip(),
                       "enablementMode": "ENABLED"}},
}

out = pathlib.Path(__file__).with_name("supply_chain.lvdash.json")   # the bundle deploys this file
out.write_text(json.dumps(dashboard, indent=1))
print("wrote", out)
