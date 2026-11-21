"""builds genie/space.json - the genie agent config. run: python3 genie/build_genie.py"""
import json, hashlib, pathlib

G = "workspace.ev_gold"
def gid(s):  # stable 32-hex ids so re-running gives the same ids
    return hashlib.md5(s.encode()).hexdigest()

SAMPLE_QUESTIONS = [
    "Which suppliers are getting worse?",
    "What is our on-time delivery by supplier region this year?",
    "Which single-sourced parts depend on a tier C supplier?",
    "What should we expedite today?",
    "Which ordering policy is cheapest at Plant 2?",
    "How accurate is the vehicle build forecast 13 weeks out?",
]

EXAMPLES = [
    ("What is on-time delivery by supplier region?",
     f"SELECT `Supplier Region`, MEASURE(`On-Time Rate`) AS on_time_rate, MEASURE(`Delivery Lines`) AS deliveries\n"
     f"FROM {G}.supplier_delivery_metrics GROUP BY ALL ORDER BY on_time_rate"),
    ("How did electronics lead times change during the chip shortage?",
     f"SELECT `Order Month`, MEASURE(`Avg Lead Time Days`) AS avg_lead_time_days, MEASURE(`On-Time Rate`) AS on_time_rate\n"
     f"FROM {G}.supplier_delivery_metrics WHERE `Component Category` = 'Electronics' GROUP BY ALL ORDER BY `Order Month`"),
    ("Which suppliers are getting worse?",
     f"SELECT supplier_name, country, tier, lateness_trend_days_per_month, on_time_rate_prior_12m, on_time_rate_last_12m, spend\n"
     f"FROM {G}.mart_supplier_scorecard WHERE is_declining ORDER BY lateness_trend_days_per_month DESC"),
    ("How much spend is at risk by category?",
     f"SELECT `Component Category`, MEASURE(`High-Risk Parts`) AS high_risk_parts, MEASURE(`Spend at Risk`) AS spend_at_risk\n"
     f"FROM {G}.sourcing_risk_metrics GROUP BY ALL ORDER BY spend_at_risk DESC"),
    ("What should we expedite today?",
     f"SELECT site_id, part_number, part_name, supplier_name, days_of_cover, safety_stock_days, suggested_order_qty, suggested_order_value\n"
     f"FROM {G}.mart_buy_signals WHERE signal = 'EXPEDITE' ORDER BY suggested_order_value DESC LIMIT 50"),
    ("Which ordering policy is cheapest at each site?",
     f"SELECT `Site`, `Policy`, MEASURE(`Total Cost`) AS total_cost, MEASURE(`Stockout Rate`) AS stockout_rate,\n"
     f"       MEASURE(`Avg Inventory Value`) AS avg_inventory_value\n"
     f"FROM {G}.policy_simulation_metrics GROUP BY ALL ORDER BY `Site`, total_cost"),
    ("How accurate is the forecast by horizon?",
     f"SELECT `Forecast Type`, `Model`, `Horizon Weeks`, MEASURE(`WAPE`) AS wape\n"
     f"FROM {G}.forecast_accuracy_metrics GROUP BY ALL ORDER BY `Forecast Type`, `Horizon Weeks`, `Model`"),
]

INSTRUCTIONS = [
    "This space covers the supply chain of an EV maker: 3 vehicle models (PU pickup, SUV, VAN delivery van), two plants "
    "(PLT1 Midwest, PLT2 Southeast - PLT2 launched in March 2026 and is still ramping) and a service parts DC (SDC1).",
    "Always use MEASURE() on the metric views for rates and averages (On-Time Rate, WAPE, Stockout Rate, Days of Cover). "
    "They are ratios of sums - never average a percentage column.",
    "On time means received on or before the promised date. Automakers expect 95%+ on-time delivery.",
    "Supplier tiers: A 97%+ on time, B 90-97%, C under 90%, Unrated = not enough history. In supplier_delivery_metrics, "
    "Supplier Tier is the tier at the time of delivery; Current Supplier Tier is today's.",
    "A declining supplier is one getting at least 0.15 days later every month over the last 24 months (is_declining).",
    "Most auto parts are single-sourced on purpose. High risk means single-sourced from a tier C supplier.",
    "Buffer classes: JIT (local body and wiring, daily deliveries, half a day of buffer), KANBAN (local fasteners, min/max), "
    "STANDARD (95% service level), STRATEGIC (battery materials and electronics, 99% service level).",
    "Buy signals: EXPEDITE = on hand below safety stock, ORDER = inventory position below the reorder point, EXCESS = over twice "
    "the max. DRR = daily run rate (forecast average daily usage).",
    "The chip shortage ran June 2021 to December 2022, worst around April 2022. It shows up in Electronics by Order Month.",
    "Ordering policies were simulated over the last 52 weeks with the same demand and late deliveries: Current, Lean, High service, "
    "EOQ, DDMRP, MRP (schedule). Lead with Stockout Rate (share of parts that ran out at least once) and Total Cost; "
    "unit fill rate always looks high because cheap high-volume parts dominate it.",
    "Forecast accuracy compares LightGBM with a naive baseline. Lower WAPE is better. 'This year' means calendar 2026.",
]

space = {
    "version": 2,
    "config": {"sample_questions": sorted([{"id": gid("q" + q), "question": [q]} for q in SAMPLE_QUESTIONS], key=lambda x: x["id"])},
    "data_sources": {
        "metric_views": sorted([{"identifier": f"{G}.{v}"} for v in [
            "supplier_delivery_metrics", "sourcing_risk_metrics", "inventory_planning_metrics",
            "forecast_accuracy_metrics", "policy_simulation_metrics"]], key=lambda x: x["identifier"]),
        "tables": sorted([
            {"identifier": f"{G}.mart_buy_signals", "column_configs": sorted([
                {"column_name": "signal", "enable_format_assistance": True, "enable_entity_matching": True},
                {"column_name": "buffer_class", "enable_format_assistance": True, "enable_entity_matching": True},
                {"column_name": "site_id", "enable_format_assistance": True, "enable_entity_matching": True}], key=lambda c: c["column_name"])},
            {"identifier": f"{G}.mart_supplier_scorecard", "column_configs": sorted([
                {"column_name": "region", "enable_format_assistance": True, "enable_entity_matching": True},
                {"column_name": "tier", "enable_format_assistance": True, "enable_entity_matching": True}], key=lambda c: c["column_name"])},
        ], key=lambda x: x["identifier"]),
    },
    "instructions": {
        "text_instructions": [{"id": gid("instructions"), "content": INSTRUCTIONS}],
        "example_question_sqls": sorted([{"id": gid("x" + q), "question": [q], "sql": [s]} for q, s in EXAMPLES], key=lambda x: x["id"]),
    },
    "benchmarks": {"questions": sorted([{"id": gid("b" + q), "question": [q], "answer": [{"format": "SQL", "content": [s]}]}
                                        for q, s in EXAMPLES[:4]], key=lambda x: x["id"])},
}
# benchmark ids must differ from sample question ids - they do, different prefixes
out = pathlib.Path(__file__).with_name("space.json")
out.write_text(json.dumps(space, indent=1))
json.dump([s for _, s in EXAMPLES], open(pathlib.Path(__file__).with_name("example_sqls.json"), "w"))
print("wrote", out)
