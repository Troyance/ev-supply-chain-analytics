"""builds dashboards/supply_chain.json - the ai/bi dashboard definition.
run: python3 dashboards/build_dashboard.py"""
import json, pathlib

def sql(*lines):
    return [l.rstrip() + "\n" for l in lines]

datasets = [
    {"name": "ds_suppliers", "displayName": "Suppliers",
     "queryLines": sql(
         "SELECT supplier_id, supplier_name, country, region, tier, po_lines, spend,",
         "       on_time_pct / 100 AS on_time_rate, on_time_pct_recent / 100 AS on_time_rate_recent,",
         "       on_time_pct_prior / 100 AS on_time_rate_prior, avg_lead_time_days, lead_time_stddev,",
         "       lateness_trend_days_per_month, is_declining,",
         "       CASE WHEN is_declining THEN 'Declining' ELSE 'Stable' END AS trend_status,",
         "       on_time_pct * po_lines / 100 AS on_time_lines",
         "FROM gold_supplier_scorecard"),
     "columns": [
         {"displayName": "Suppliers", "description": "Number of suppliers", "expression": "COUNT(`supplier_id`)"},
         {"displayName": "Declining Suppliers", "description": "Suppliers getting 1+ day later per month",
          "expression": "SUM(CASE WHEN `is_declining` THEN 1 ELSE 0 END)"},
         {"displayName": "Total Spend", "description": "Net spend, USD", "expression": "SUM(`spend`)"},
         {"displayName": "On-Time Rate", "description": "Share of PO lines received by the promised date",
          "expression": "SUM(`on_time_lines`) / SUM(`po_lines`)"},
     ]},
    {"name": "ds_declining", "displayName": "Declining suppliers",
     "queryLines": sql(
         "SELECT supplier_name, country, region, tier, spend, on_time_pct_prior / 100 AS on_time_prior,",
         "       on_time_pct_recent / 100 AS on_time_recent, lateness_trend_days_per_month",
         "FROM gold_supplier_scorecard WHERE is_declining",
         "ORDER BY lateness_trend_days_per_month DESC")},
    {"name": "ds_monthly", "displayName": "Monthly performance",
     "queryLines": sql(
         "SELECT order_month, component_category, po_lines, spend,",
         "       on_time_pct * po_lines / 100 AS on_time_lines, avg_lead_time_days * po_lines AS lead_days",
         "FROM gold_monthly_performance"),
     "columns": [
         {"displayName": "On-Time Rate", "description": "Share of lines on time", "expression": "SUM(`on_time_lines`) / SUM(`po_lines`)"},
         {"displayName": "Avg Lead Time", "description": "Order-to-receipt days, weighted by lines", "expression": "SUM(`lead_days`) / SUM(`po_lines`)"},
     ]},
    {"name": "ds_risk", "displayName": "Single-source risk",
     "queryLines": sql(
         "SELECT part_id, part_name, component_category, top_supplier_name, top_supplier_country,",
         "       top_supplier_tier, top_supplier_on_time_pct / 100 AS top_supplier_on_time, total_spend,",
         "       top_supplier_share_pct / 100 AS top_supplier_share, is_high_risk,",
         "       CASE WHEN is_high_risk THEN 'High risk (tier C supplier)' ELSE 'Concentrated' END AS risk_level",
         "FROM gold_single_source_risk"),
     "columns": [
         {"displayName": "Single-Sourced Parts", "description": "Parts with one supplier at 80%+ of spend", "expression": "COUNT(`part_id`)"},
         {"displayName": "High-Risk Parts", "description": "Single-sourced from a tier C supplier",
          "expression": "SUM(CASE WHEN `is_high_risk` THEN 1 ELSE 0 END)"},
         {"displayName": "Spend at Risk", "description": "Spend on high-risk parts, USD",
          "expression": "SUM(CASE WHEN `is_high_risk` THEN `total_spend` ELSE 0 END)"},
     ]},
    {"name": "ds_risk_top", "displayName": "Top high-risk parts",
     "queryLines": sql(
         "SELECT part_number, part_name, component_category, top_supplier_name, top_supplier_country,",
         "       top_supplier_on_time_pct / 100 AS top_supplier_on_time, top_supplier_share_pct / 100 AS share, total_spend",
         "FROM gold_single_source_risk WHERE is_high_risk ORDER BY total_spend DESC LIMIT 200")},
    {"name": "ds_geo", "displayName": "Geography",
     "queryLines": sql(
         "SELECT region, country, suppliers, spend, spend_share_pct / 100 AS spend_share,",
         "       avg_lead_time_days * spend AS lead_x_spend",
         "FROM gold_spend_by_geography"),
     "columns": [
         {"displayName": "Avg Lead Time", "description": "Spend-weighted order-to-receipt days", "expression": "SUM(`lead_x_spend`) / SUM(`spend`)"},
     ]},
    {"name": "ds_builds", "displayName": "Builds: actual + forecast",
     "queryLines": sql(
         "WITH latest AS (SELECT run_id FROM forecast_run WHERE run_type = 'production' AND forecast_type = 'vehicle_builds'",
         "                ORDER BY created_at DESC LIMIT 1)",
         "SELECT week_start AS week, site_id || ' ' || model_id AS line, units AS actual,",
         "       CAST(NULL AS DOUBLE) AS forecast, CAST(NULL AS DOUBLE) AS forecast_low, CAST(NULL AS DOUBLE) AS forecast_high",
         "FROM fct_vehicle_builds WHERE week_start >= '2024-01-01'",
         "UNION ALL",
         "SELECT target_week, site_id || ' ' || model_id, NULL, forecast_p50, forecast_p10, forecast_p90",
         "FROM fct_forecast JOIN latest USING (run_id)")},
    {"name": "ds_accuracy", "displayName": "Forecast accuracy",
     "queryLines": sql(
         "SELECT forecast_type, CASE WHEN model_name = 'naive_mean' THEN 'Naive baseline' ELSE 'LightGBM' END AS model,",
         "       horizon_weeks, abs_error, actual_units",
         "FROM gold_forecast_accuracy"),
     "columns": [
         {"displayName": "WAPE", "description": "Weighted absolute percent error", "expression": "SUM(`abs_error`) / SUM(`actual_units`)"},
     ]},
    {"name": "ds_signals", "displayName": "Buy signals",
     "queryLines": sql(
         "SELECT site_id, part_id, part_number, part_name, component_category, buffer_class, supplier_name, supplier_region AS region,",
         "       supplier_tier, daily_run_rate, planning_lead_time_days, safety_stock_days, days_of_cover,",
         "       on_hand_qty, on_order_qty, reorder_point, signal, suggested_order_qty, suggested_order_value,",
         "       projected_stockout_date, expected_receipt_date,",
         "       CASE signal WHEN 'EXPEDITE' THEN 1 WHEN 'ORDER' THEN 2 WHEN 'EXCESS' THEN 3 ELSE 4 END AS signal_rank",
         "FROM gold_buy_signals"),
     "columns": [
         {"displayName": "Parts to Expedite", "description": "On hand below safety stock",
          "expression": "SUM(CASE WHEN `signal` = 'EXPEDITE' THEN 1 ELSE 0 END)"},
         {"displayName": "Parts to Order", "description": "Inventory position below reorder point",
          "expression": "SUM(CASE WHEN `signal` = 'ORDER' THEN 1 ELSE 0 END)"},
         {"displayName": "Suggested Order Value", "description": "USD", "expression": "SUM(`suggested_order_value`)"},
     ]},
]


datasets += [
    {"name": "ds_policy", "displayName": "Policy comparison",
     "queryLines": sql(
         "SELECT site_id, part_id, concat(site_id, ' ', part_number, ' ', part_name) AS part, buffer_class, component_category,",
         "       supplier_region AS region, CASE policy WHEN 'current' THEN 'Current' WHEN 'lean' THEN 'Lean' WHEN 'high_service' THEN 'High service' WHEN 'eoq' THEN 'EOQ' WHEN 'ddmrp' THEN 'DDMRP' ELSE 'MRP (schedule)' END AS policy,",
         "       holding_cost, ordering_cost, shortage_cost, total_cost, avg_inventory_value, units_short, demand_units,",
         "       orders_placed, stockout_days",
         "FROM gold_policy_comparison"),
     "columns": [
         {"displayName": "Total Cost", "description": "Holding + ordering + shortage, USD", "expression": "SUM(`total_cost`)"},
         {"displayName": "Fill Rate", "description": "Share of demand met from stock", "expression": "1 - SUM(`units_short`) / SUM(`demand_units`)"},
         {"displayName": "Avg Inventory Value", "description": "Average stock on hand, USD", "expression": "SUM(`avg_inventory_value`)"},
     ]},
    {"name": "ds_sawtooth", "displayName": "Policy sawtooth",
     "queryLines": sql(
         "SELECT concat(d.site_id, ' ', p.part_number, ' ', p.part_name) AS part, d.day,",
         "       CASE d.policy WHEN 'current' THEN 'Current' WHEN 'lean' THEN 'Lean' WHEN 'high_service' THEN 'High service'",
         "         WHEN 'eoq' THEN 'EOQ' WHEN 'ddmrp' THEN 'DDMRP' ELSE 'MRP (schedule)' END AS policy, d.on_hand, d.unmet",
         "FROM gold_policy_simulation_daily d JOIN dim_part_planning p USING (part_id)")},
    {"name": "ds_projection", "displayName": "13-week projection",
     "queryLines": sql(
         "SELECT concat(j.site_id, ' ', p.part_number, ' ', p.part_name) AS part, j.day, j.on_hand, j.reorder_point,",
         "       j.safety_stock, j.order_placed",
         "FROM gold_inventory_projection j JOIN dim_part_planning p USING (part_id)")},
]

# ---------- widget helpers ----------
def f(name, expr):
    return {"name": name, "expression": expr}

def q(ds, fields, disagg=False, orders=None):
    query = {"datasetName": ds, "fields": fields, "disaggregated": disagg}
    if orders:
        query["orders"] = orders
    return [{"name": "main_query", "query": query}]

def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}

def text(name, md, p):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [md]}}, "position": p}

USD = {"type": "number-currency", "currencyCode": "USD", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}
NUM = {"type": "number-plain", "decimalPlaces": {"type": "max", "places": 1}}

def counter(name, ds, measure, title, p, fmt=None):
    value = {"fieldName": f"measure({measure})", "displayName": title}
    if fmt:
        value["format"] = fmt
    return {"widget": {"name": name,
                       "queries": q(ds, [f(f"measure({measure})", f"MEASURE(`{measure}`)")]),
                       "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": value},
                                "frame": {"title": title, "showTitle": True}}},
            "position": p}

def chart(name, kind, ds, x, y, title, p, color=None, extra_spec=None, desc=None):
    """x, y, color are (field_name, expression, scale_type, display, format)"""
    fields = [f(x[0], x[1]), f(y[0], y[1])] + ([f(color[0], color[1])] if color else [])
    enc = {"x": {"fieldName": x[0], "scale": {"type": x[2]}, "displayName": x[3]},
           "y": {"fieldName": y[0], "scale": {"type": y[2]}, "displayName": y[3]}}
    for axis, spec in (("x", x), ("y", y)):
        if len(spec) > 4 and spec[4]:
            enc[axis]["format"] = spec[4]
    if color:
        enc["color"] = {"fieldName": color[0], "scale": {"type": "categorical", **({"mappings": color[4]} if len(color) > 4 else {})},
                        "displayName": color[3]}
    spec = {"version": 3, "widgetType": kind, "encodings": enc, "frame": {"title": title, "showTitle": True}}
    if desc:
        spec["frame"].update({"showDescription": True, "description": desc})
    if extra_spec:
        spec.update(extra_spec)
    return {"widget": {"name": name, "queries": q(ds, fields), "spec": spec}, "position": p}

def table(name, ds, cols, title, p):
    """cols: (field, display, format or None)"""
    columns = []
    for c in cols:
        col = {"fieldName": c[0], "displayName": c[1]}
        if len(c) > 2 and c[2]:
            col["format"] = c[2]
        columns.append(col)
    return {"widget": {"name": name, "queries": q(ds, [f(c[0], f"`{c[0]}`") for c in cols], disagg=True),
                       "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": columns},
                                "frame": {"title": title, "showTitle": True}}},
            "position": p}

def page(name, display, layout):
    return {"name": name, "displayName": display, "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": layout}

BAD, WARN, GOOD, MUTED = "#E15759", "#F28E2C", "#59A14F", "#9AA5B1"
month = ("monthly(order_month)", 'DATE_TRUNC("MONTH", `order_month`)', "temporal", "Order month")
def marker(date, label, color):
    return {"type": "vertical-line", "encodings": {"x": {"dataValue": f"{date}T00:00:00.000", "dataType": "DATETIME"},
                                                    "label": {"value": label}, "color": {"value": color}}}

chip_shortage = [marker("2021-06-01", "Chip shortage starts", BAD), marker("2022-04-01", "Worst point", BAD),
                 marker("2022-12-31", "Shortage over", GOOD)]

# ---------- page 1: suppliers ----------
suppliers = [
    text("title-suppliers", "# Supplier performance\nHow reliable the supplier base is, where it is slow, and who is getting worse. "
         "Asia takes about 4x longer than north america. Electronics lead times and lateness spiked during the 2021-22 chip shortage.", pos(0, 0, 12, 2)),
    counter("kpi-spend", "ds_suppliers", "Total Spend", "Total spend", pos(0, 2, 3, 3), USD),
    counter("kpi-ontime", "ds_suppliers", "On-Time Rate", "On-time delivery", pos(3, 2, 3, 3), PCT),
    counter("kpi-suppliers", "ds_suppliers", "Suppliers", "Suppliers", pos(6, 2, 3, 3), {"type": "number-plain", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}),
    counter("kpi-declining", "ds_suppliers", "Declining Suppliers", "Declining suppliers", pos(9, 2, 3, 3)),
    chart("lead-time-trend", "line", "ds_monthly", month,
          ("measure(Avg Lead Time)", "MEASURE(`Avg Lead Time`)", "quantitative", "Avg lead time (days)", NUM),
          "Lead time by component category", pos(0, 5, 8, 6),
          color=("component_category", "`component_category`", "categorical", "Category"),
          extra_spec={"annotations": chip_shortage}),
    chart("lead-by-region", "bar", "ds_geo",
          ("region", "`region`", "categorical", "Region"),
          ("measure(Avg Lead Time)", "MEASURE(`Avg Lead Time`)", "quantitative", "Avg lead time (days)", NUM),
          "Lead time by supplier region", pos(8, 5, 4, 6)),
    chart("ontime-trend", "line", "ds_monthly", month,
          ("measure(On-Time Rate)", "MEASURE(`On-Time Rate`)", "quantitative", "On-time rate", PCT),
          "On-time delivery over time", pos(0, 11, 6, 5)),
    chart("tier-by-region", "bar", "ds_suppliers",
          ("region", "`region`", "categorical", "Region"),
          ("measure(Suppliers)", "MEASURE(`Suppliers`)", "quantitative", "Suppliers"),
          "Supplier tiers by region", pos(6, 11, 6, 5),
          color=("tier", "`tier`", "categorical", "Tier",
                 [{"value": "A", "color": GOOD}, {"value": "B", "color": WARN}, {"value": "C", "color": BAD}])),
    table("declining-table", "ds_declining", [
        ("supplier_name", "Supplier"), ("country", "Country"), ("tier", "Tier"),
        ("lateness_trend_days_per_month", "Days later / month", NUM),
        ("on_time_prior", "On-time (prior 12m)", PCT), ("on_time_recent", "On-time (last 12m)", PCT),
        ("spend", "Spend", USD)], "Declining suppliers (getting 1+ day later every month)", pos(0, 16, 12, 7)),
]

# ---------- page 2: sourcing risk ----------
risk = [
    text("title-risk", "# Sourcing risk\nParts where one supplier gets 80%+ of the spend. High risk means that supplier is also tier C. "
         "Most auto parts are single-sourced on purpose, so the question is which ones depend on an unreliable supplier.", pos(0, 0, 12, 2)),
    counter("kpi-single", "ds_risk", "Single-Sourced Parts", "Single-sourced parts", pos(0, 2, 4, 3), {"type": "number-plain", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}),
    counter("kpi-highrisk", "ds_risk", "High-Risk Parts", "High-risk parts", pos(4, 2, 4, 3), {"type": "number-plain", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}),
    counter("kpi-spend-risk", "ds_risk", "Spend at Risk", "Spend at risk", pos(8, 2, 4, 3), USD),
    chart("risk-by-category", "bar", "ds_risk",
          ("component_category", "`component_category`", "categorical", "Category"),
          ("measure(Single-Sourced Parts)", "MEASURE(`Single-Sourced Parts`)", "quantitative", "Parts"),
          "Single-sourced parts by category", pos(0, 5, 6, 6),
          color=("risk_level", "`risk_level`", "categorical", "Risk",
                 [{"value": "High risk (tier C supplier)", "color": BAD}, {"value": "Concentrated", "color": WARN}])),
    chart("spend-share-region", "pie", "ds_geo",
          ("region", "`region`", "categorical", "Region"),
          ("sum(spend)", "SUM(`spend`)", "quantitative", "Spend"),
          "Spend by supplier region", pos(6, 5, 6, 6)),
    table("risk-table", "ds_risk_top", [
        ("part_number", "Part #"), ("part_name", "Part"), ("component_category", "Category"), ("top_supplier_name", "Supplier"),
        ("top_supplier_country", "Country"), ("top_supplier_on_time", "Supplier on-time", PCT),
        ("share", "Supplier share", PCT), ("total_spend", "Spend", USD)],
        "Biggest high-risk parts", pos(0, 11, 12, 7)),
]
# pie uses angle/color instead of x/y
pie = risk[5]["widget"]["spec"]
pie["encodings"] = {"angle": {"fieldName": "sum(spend)", "scale": {"type": "quantitative"}, "displayName": "Spend"},
                    "color": {"fieldName": "region", "scale": {"type": "categorical"}, "displayName": "Region"},
                    "label": {"show": True}}

# ---------- page 3: planning ----------
week = ("weekly(week)", 'DATE_TRUNC("WEEK", `week`)', "temporal", "Week")
planning = [
    text("title-planning", "# Forecast & buy signals\nVehicle builds forecast 13 weeks out (LightGBM), exploded through the BOM into part demand, "
         "then compared with stock on hand to decide what to order. Plant 2 is still ramping up.", pos(0, 0, 12, 2)),
    counter("kpi-expedite", "ds_signals", "Parts to Expedite", "Parts to expedite", pos(0, 2, 4, 3)),
    counter("kpi-order", "ds_signals", "Parts to Order", "Parts to order", pos(4, 2, 4, 3)),
    counter("kpi-order-value", "ds_signals", "Suggested Order Value", "Suggested order value", pos(8, 2, 4, 3), USD),
    chart("builds-forecast", "line", "ds_builds", week,
          ("sum(actual)", "SUM(`actual`)", "quantitative", "Actual builds"),
          "Weekly vehicle builds: actual and forecast", pos(0, 5, 8, 6),
          color=("line", "`line`", "categorical", "Plant / model")),
    chart("accuracy-by-horizon", "line", "ds_accuracy",
          ("horizon_weeks", "`horizon_weeks`", "quantitative", "Weeks ahead"),
          ("measure(WAPE)", "MEASURE(`WAPE`)", "quantitative", "WAPE", PCT),
          "Forecast error by horizon (backtest)", pos(8, 5, 4, 6),
          color=("model", "`model`", "categorical", "Model",
                 [{"value": "LightGBM", "color": GOOD}, {"value": "Naive baseline", "color": MUTED}]),
          desc="Lower is better. Weighted absolute percent error vs actuals."),
    chart("signals-by-class", "bar", "ds_signals",
          ("buffer_class", "`buffer_class`", "categorical", "Buffer class"),
          ("count(part_id)", "COUNT(`part_id`)", "quantitative", "Parts"),
          "Buy signals by buffer class", pos(0, 11, 12, 5),
          color=("signal", "`signal`", "categorical", "Signal",
                 [{"value": "EXPEDITE", "color": BAD}, {"value": "ORDER", "color": WARN},
                  {"value": "OK", "color": GOOD}, {"value": "EXCESS", "color": MUTED}])),
    table("signals-table", "ds_signals", [
        ("signal", "Signal"), ("site_id", "Site"), ("part_number", "Part #"), ("part_name", "Part"), ("component_category", "Category"),
        ("buffer_class", "Buffer"), ("supplier_name", "Supplier"), ("daily_run_rate", "DRR", NUM),
        ("days_of_cover", "Days of cover", NUM), ("planning_lead_time_days", "Lead time (d)"),
        ("safety_stock_days", "Safety stock (d)", NUM), ("suggested_order_qty", "Order qty"),
        ("suggested_order_value", "Order value", USD), ("projected_stockout_date", "Stockout date")],
        "Buy signals", pos(0, 16, 12, 8)),
]
# builds chart: plot actual and forecast as two y series, split by plant/model
b = planning[4]["widget"]
b["queries"][0]["query"]["fields"] = [f(week[0], week[1]), f("sum(actual)", "SUM(`actual`)"),
                                      f("sum(forecast)", "SUM(`forecast`)"), f("line", "`line`")]
b["spec"]["encodings"]["y"] = {"scale": {"type": "quantitative"}, "fields": [
    {"fieldName": "sum(actual)", "displayName": "Actual"}, {"fieldName": "sum(forecast)", "displayName": "Forecast"}]}
b["spec"]["encodings"].pop("color")
b["spec"]["facet"] = {"type": "wrap", "fieldName": "line", "scale": {"type": "categorical"},
                      "layout": {"column": {"type": "count", "value": 2}}}
# signals table: most urgent first
planning[7]["widget"]["queries"][0]["query"]["orders"] = [
    {"direction": "ASC", "expression": "`signal_rank`"}, {"direction": "DESC", "expression": "`suggested_order_value`"}]
planning[7]["widget"]["queries"][0]["query"]["fields"].append(f("signal_rank", "`signal_rank`"))

# ---------- filters ----------
def filt(name, kind, field, title, ds_list, p):
    queries = [{"name": f"{name}_{ds}", "query": {"datasetName": ds, "fields": [f(field, f"`{field}`")], "disaggregated": False}}
               for ds in ds_list]
    return {"widget": {"name": name, "queries": queries,
                       "spec": {"version": 2, "widgetType": kind,
                                "encodings": {"fields": [{"fieldName": field, "displayName": title, "queryName": qq["name"]} for qq in queries]},
                                "frame": {"showTitle": True, "title": title}}},
            "position": p}


# ---------- page 4: ordering policies ----------
DEFAULT_PART = "PLT1 BDY-79568 Fender liner"
day = ("day", "`day`", "temporal", "Day")
POLICY_COLORS = [{"value": "Current", "color": "#4E79A7"}, {"value": "Lean", "color": BAD},
                 {"value": "High service", "color": GOOD}, {"value": "EOQ", "color": "#B07AA1"},
                 {"value": "DDMRP", "color": WARN}, {"value": "MRP (schedule)", "color": "#76B7B2"}]
policies = [
    text("title-policies", "# Ordering policies\nThe last 52 weeks of real demand replayed under six ordering policies - same demand, same late "
         "deliveries, same starting point. Shortages dominate the bill: a vehicle has ~1,300 parts, so 99.7% availability per part is not enough. "
         "Plant 2 is ramping, and only schedule-driven MRP keeps up - trailing averages always lag a ramp.", pos(0, 0, 12, 2)),
    chart("policy-cost", "bar", "ds_policy",
          ("policy", "`policy`", "categorical", "Policy"),
          ("sum(holding_cost)", "SUM(`holding_cost`)", "quantitative", "Cost", USD),
          "Annual cost by policy", pos(0, 2, 6, 6)),
    chart("policy-fill", "bar", "ds_policy",
          ("policy", "`policy`", "categorical", "Policy"),
          ("measure(Fill Rate)", "MEASURE(`Fill Rate`)", "quantitative", "Fill rate", PCT),
          "Fill rate by policy", pos(6, 2, 3, 6),
          color=("policy", "`policy`", "categorical", "Policy", POLICY_COLORS)),
    chart("policy-inventory", "bar", "ds_policy",
          ("policy", "`policy`", "categorical", "Policy"),
          ("measure(Avg Inventory Value)", "MEASURE(`Avg Inventory Value`)", "quantitative", "Avg inventory", USD),
          "Cash tied up in stock", pos(9, 2, 3, 6),
          color=("policy", "`policy`", "categorical", "Policy", POLICY_COLORS)),
    chart("policy-by-site", "bar", "ds_policy",
          ("site_id", "`site_id`", "categorical", "Site"),
          ("measure(Total Cost)", "MEASURE(`Total Cost`)", "quantitative", "Total cost", USD),
          "Total cost by site and policy", pos(0, 8, 6, 6),
          color=("policy", "`policy`", "categorical", "Policy", POLICY_COLORS),
          extra_spec={"mark": {"layout": "group"}}),
    chart("policy-by-class", "bar", "ds_policy",
          ("buffer_class", "`buffer_class`", "categorical", "Buffer class"),
          ("measure(Total Cost)", "MEASURE(`Total Cost`)", "quantitative", "Total cost", USD),
          "Total cost by buffer class and policy", pos(6, 8, 6, 6),
          color=("policy", "`policy`", "categorical", "Policy", POLICY_COLORS),
          extra_spec={"mark": {"layout": "group"}}),
    chart("sawtooth-policies", "line", "ds_sawtooth", day,
          ("sum(on_hand)", "SUM(`on_hand`)", "quantitative", "On hand (units)"),
          "Sawtooth: on-hand stock for the selected part under each policy", pos(0, 14, 12, 7),
          color=("policy", "`policy`", "categorical", "Policy", POLICY_COLORS)),
    chart("sawtooth-projection", "line", "ds_projection", day,
          ("sum(on_hand)", "SUM(`on_hand`)", "quantitative", "Units"),
          "Next 13 weeks from today's stock (current policy)", pos(0, 21, 12, 6)),
]
# cost bar: stack the three cost types
c = policies[1]["widget"]
c["queries"][0]["query"]["fields"] = [f("policy", "`policy`"), f("sum(holding_cost)", "SUM(`holding_cost`)"),
                                      f("sum(ordering_cost)", "SUM(`ordering_cost`)"), f("sum(shortage_cost)", "SUM(`shortage_cost`)")]
c["spec"]["encodings"]["y"] = {"scale": {"type": "quantitative"}, "format": USD, "fields": [
    {"fieldName": "sum(holding_cost)", "displayName": "Holding"}, {"fieldName": "sum(ordering_cost)", "displayName": "Ordering"},
    {"fieldName": "sum(shortage_cost)", "displayName": "Shortage"}]}
# projection: on hand vs reorder point vs safety stock
pj = policies[7]["widget"]
pj["queries"][0]["query"]["fields"] = [f("day", "`day`"), f("sum(on_hand)", "SUM(`on_hand`)"),
                                       f("sum(reorder_point)", "SUM(`reorder_point`)"), f("sum(safety_stock)", "SUM(`safety_stock`)")]
pj["spec"]["encodings"]["y"] = {"scale": {"type": "quantitative"}, "fields": [
    {"fieldName": "sum(on_hand)", "displayName": "On hand"}, {"fieldName": "sum(reorder_point)", "displayName": "Reorder point"},
    {"fieldName": "sum(safety_stock)", "displayName": "Safety stock"}]}

# page-level part picker driving both sawtooth charts
part_filter = filt("filter-part", "filter-single-select", "part", "Part", ["ds_sawtooth", "ds_projection"], pos(0, 27, 12, 2))
part_filter["widget"]["spec"]["selection"] = {"defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": DEFAULT_PART}]}}}
policies.insert(1, {**part_filter, "position": pos(0, 2, 12, 2)})
for w in policies[2:]:
    w["position"]["y"] += 2

filters = {"name": "filters", "displayName": "Filters", "pageType": "PAGE_TYPE_GLOBAL_FILTERS", "layoutVersion": "GRID_V1",
           "layout": [
               filt("filter-region", "filter-multi-select", "region", "Supplier region", ["ds_suppliers", "ds_declining", "ds_geo", "ds_signals"], pos(0, 0, 4, 2)),
               filt("filter-category", "filter-multi-select", "component_category", "Component category", ["ds_monthly", "ds_risk", "ds_risk_top", "ds_signals"], pos(4, 0, 4, 2)),
               filt("filter-month", "filter-date-range-picker", "order_month", "Order month", ["ds_monthly"], pos(8, 0, 4, 2)),
           ]}

dashboard = {
    "datasets": datasets,
    "pages": [page("suppliers", "Suppliers", suppliers), page("risk", "Sourcing risk", risk),
              page("planning", "Forecast & buy signals", planning), page("policies", "Ordering policies", policies), filters],
    "uiSettings": {"theme": {
        "canvasBackgroundColor": {"light": "#F7F8FA", "dark": "#1B2128"},
        "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"},
        "widgetBorderColor": {"light": "#FFFFFF", "dark": "#11171C"},
        "fontColor": {"light": "#11171C", "dark": "#E8ECF0"},
        "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"},
        "visualizationColors": ["#4E79A7", "#F28E2C", "#76B7B2", "#B07AA1", "#EDC948", "#9C755F", "#59A14F"],
        "widgetHeaderAlignment": "LEFT"}},
}

out = pathlib.Path(__file__).with_name("supply_chain.json")
out.write_text(json.dumps(dashboard, indent=1))
print("wrote", out)
