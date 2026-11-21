-- semantic layer: one metric view per kpi group. ratios are defined as measure / measure so they
-- re-aggregate correctly at any grain (on-time rate by month, by supplier, by region... all from the same definition).

CREATE OR REPLACE VIEW workspace.ev_supply_chain.supplier_delivery_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: workspace.ev_supply_chain.fct_po_lines_scenario
comment: "Supplier delivery performance and spend, one row per PO delivery line. Use for on-time delivery, lead time, lateness and spend by supplier, region, part category, plant or month."
joins:
  - name: dim_supplier
    source: workspace.ev_supply_chain.dim_supplier
    'on': source.supplier_id = dim_supplier.supplier_id
  - name: dim_scorecard
    source: workspace.ev_supply_chain.gold_supplier_scorecard
    'on': source.supplier_id = dim_scorecard.supplier_id
  - name: dim_part
    source: workspace.ev_supply_chain.dim_part_catalog
    'on': source.part_id = dim_part.part_id
dimensions:
  - name: Supplier
    expr: dim_supplier.supplier_name
    comment: "Supplier name."
    display_name: "Supplier"
    synonyms: ["vendor", "supplier name"]
  - name: Supplier Country
    expr: dim_supplier.country
    comment: "Country the supplier ships from."
    display_name: "Supplier country"
    synonyms: ["country", "origin country"]
  - name: Supplier Region
    expr: dim_supplier.region
    comment: "World region the supplier ships from: America, Asia, Europe, Africa, Middle East."
    display_name: "Supplier region"
    synonyms: ["region", "sourcing region"]
  - name: Supplier Tier
    expr: dim_scorecard.tier
    comment: "Reliability tier: A (97 percent or better on time), B (90-97), C (under 90)."
    display_name: "Supplier tier"
    synonyms: ["tier", "rating"]
  - name: Is Declining Supplier
    expr: dim_scorecard.is_declining
    comment: "True for suppliers getting at least 0.15 days later every month over the last 24 months."
    display_name: "Declining supplier"
    synonyms: ["getting worse", "slipping"]
  - name: Component Category
    expr: dim_part.component_category
    comment: "Battery materials, Electronics, Wiring & harnesses, Body & chassis, Fasteners & fittings."
    display_name: "Component category"
    synonyms: ["category", "commodity"]
  - name: Part
    expr: "concat(dim_part.part_number, ' ', dim_part.part_name)"
    comment: "Part number and name."
    display_name: "Part"
    synonyms: ["part number", "component", "sku"]
  - name: Plant
    expr: source.site_id
    comment: "Receiving plant: PLT1 (Midwest) or PLT2 (Southeast)."
    display_name: "Plant"
    synonyms: ["site", "factory"]
  - name: Sourcing Role
    expr: source.sourcing_role
    comment: "primary or secondary source for the part."
    display_name: "Sourcing role"
  - name: Ship Mode
    expr: source.ship_mode
    comment: "Truck, Rail or Ship."
    display_name: "Ship mode"
    synonyms: ["freight mode", "transport mode"]
  - name: Order Month
    expr: "DATE_TRUNC('MONTH', source.order_date)"
    comment: "Month the order was placed."
    display_name: "Order month"
  - name: Receipt Month
    expr: "DATE_TRUNC('MONTH', source.received_date)"
    comment: "Month the goods arrived. Use this for spend and delivery trends."
    display_name: "Receipt month"
    synonyms: ["delivery month", "month"]
measures:
  - name: Delivery Lines
    expr: COUNT(1)
    comment: "Number of PO delivery lines received."
    display_name: "Delivery lines"
    synonyms: ["deliveries", "receipts", "po lines"]
  - name: On-Time Lines
    expr: COUNT(1) FILTER (WHERE source.is_on_time)
    comment: "Delivery lines received on or before the promised date."
    display_name: "On-time lines"
  - name: Late Lines
    expr: COUNT(1) FILTER (WHERE NOT source.is_on_time)
    comment: "Delivery lines received after the promised date."
    display_name: "Late lines"
  - name: On-Time Rate
    expr: "MEASURE(`On-Time Lines`) / MEASURE(`Delivery Lines`)"
    comment: "Share of delivery lines received on time, 0-1. The main supplier reliability KPI (OTD)."
    display_name: "On-time delivery"
    synonyms: ["OTD", "on time percent", "on-time delivery", "reliability"]
    format:
      type: percentage
      decimal_places: {type: max, places: 1}
  - name: Spend
    expr: SUM(source.net_amount)
    comment: "Net spend in USD."
    display_name: "Spend"
    synonyms: ["cost", "purchase value", "dollars"]
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
  - name: Units Received
    expr: SUM(source.quantity)
    comment: "Units delivered."
    display_name: "Units received"
    synonyms: ["quantity", "volume"]
  - name: Avg Lead Time Days
    expr: AVG(source.lead_time_days)
    comment: "Average days from order placed to goods received."
    display_name: "Avg lead time (days)"
    synonyms: ["lead time", "order to receipt"]
  - name: Avg Days Late When Late
    expr: AVG(source.days_late) FILTER (WHERE source.days_late > 0)
    comment: "For late lines only, average days past the promised date."
    display_name: "Avg days late (late lines)"
    synonyms: ["lateness", "how late"]
  - name: Active Suppliers
    expr: COUNT(DISTINCT source.supplier_id)
    comment: "Number of distinct suppliers with deliveries."
    display_name: "Active suppliers"
    synonyms: ["supplier count", "number of suppliers"]
$$;

CREATE OR REPLACE VIEW workspace.ev_supply_chain.forecast_accuracy_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: workspace.ev_supply_chain.gold_forecast_accuracy
comment: "Backtest forecast accuracy for vehicle builds and service parts, LightGBM vs a naive baseline. Use WAPE to compare models, horizons, sites and categories."
dimensions:
  - name: Forecast Type
    expr: forecast_type
    comment: "vehicle_builds (weekly units per plant and model) or service_parts (weekly units per part at the service DC)."
    display_name: "Forecast type"
  - name: Model
    expr: "CASE WHEN model_name = 'naive_mean' THEN 'Naive baseline' ELSE 'LightGBM' END"
    comment: "LightGBM or the naive baseline (flat recent average)."
    display_name: "Model"
    synonyms: ["forecast model", "method"]
  - name: Horizon Weeks
    expr: horizon_weeks
    comment: "How many weeks ahead the forecast was made, 1-13."
    display_name: "Weeks ahead"
    synonyms: ["horizon", "lead", "lag"]
  - name: Target Week
    expr: target_week
    comment: "Week being forecast."
    display_name: "Target week"
  - name: Site
    expr: site_id
    comment: "PLT1, PLT2 or SDC1."
    display_name: "Site"
  - name: Vehicle Model
    expr: model_id
    comment: "PU, SUV or VAN for build forecasts. Null for service parts."
    display_name: "Vehicle model"
  - name: Component Category
    expr: component_category
    comment: "Part category for service parts, Vehicle for build forecasts."
    display_name: "Category"
measures:
  - name: Actual Units
    expr: SUM(actual_units)
    comment: "What actually happened."
    display_name: "Actual units"
  - name: Forecast Units
    expr: SUM(forecast_p50)
    comment: "Point forecast."
    display_name: "Forecast units"
  - name: Absolute Error
    expr: SUM(abs_error)
    comment: "Sum of absolute forecast errors in units."
    display_name: "Absolute error"
  - name: WAPE
    expr: "MEASURE(`Absolute Error`) / MEASURE(`Actual Units`)"
    comment: "Weighted absolute percent error, 0-1. Lower is better. The headline forecast accuracy KPI."
    display_name: "WAPE"
    synonyms: ["forecast error", "accuracy", "error rate", "mape"]
    format:
      type: percentage
      decimal_places: {type: max, places: 1}
  - name: Bias
    expr: "(MEASURE(`Forecast Units`) - MEASURE(`Actual Units`)) / MEASURE(`Actual Units`)"
    comment: "Over or under forecasting as a share of actuals. Positive means forecasts run high."
    display_name: "Bias"
    synonyms: ["over forecast", "under forecast"]
    format:
      type: percentage
      decimal_places: {type: max, places: 1}
  - name: Interval Coverage
    expr: AVG(CASE WHEN within_interval THEN 1.0 WHEN within_interval = false THEN 0.0 END)
    comment: "Share of actuals that landed inside the p10-p90 range. About 0.8 means the range is well calibrated. Null for the baseline."
    display_name: "p10-p90 coverage"
    format:
      type: percentage
      decimal_places: {type: max, places: 1}
$$;

CREATE OR REPLACE VIEW workspace.ev_supply_chain.inventory_planning_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: workspace.ev_supply_chain.gold_buy_signals
comment: "Current inventory position and replenishment recommendations, one row per site and part. Use for what to order, what to expedite, inventory value and days of cover."
dimensions:
  - name: Site
    expr: site_id
    comment: "PLT1 (Plant 1 Midwest), PLT2 (Plant 2 Southeast) or SDC1 (Service Parts DC)."
    display_name: "Site"
    synonyms: ["plant", "location", "warehouse"]
  - name: Signal
    expr: signal
    comment: "EXPEDITE (on hand below safety stock), ORDER (below reorder point), OK, EXCESS (over twice the max), NO DEMAND."
    display_name: "Signal"
    synonyms: ["action", "buy signal", "recommendation"]
  - name: Buffer Class
    expr: buffer_class
    comment: "JIT, KANBAN, STANDARD or STRATEGIC replenishment strategy."
    display_name: "Buffer class"
    synonyms: ["replenishment strategy", "inventory policy"]
  - name: Component Category
    expr: component_category
    comment: "Part category."
    display_name: "Component category"
    synonyms: ["category"]
  - name: Part
    expr: "concat(part_number, ' ', part_name)"
    comment: "Part number and name."
    display_name: "Part"
    synonyms: ["part number", "sku"]
  - name: Supplier
    expr: supplier_name
    comment: "Main supplier for the part."
    display_name: "Supplier"
    synonyms: ["vendor"]
  - name: Supplier Region
    expr: supplier_region
    comment: "Region the main supplier ships from."
    display_name: "Supplier region"
  - name: Supplier Tier
    expr: supplier_tier
    comment: "Main supplier reliability tier A, B or C."
    display_name: "Supplier tier"
  - name: Demand Type
    expr: demand_type
    comment: "dependent (plant demand from builds x BOM) or independent (service parts)."
    display_name: "Demand type"
measures:
  - name: Parts
    expr: COUNT(1)
    comment: "Site and part combinations."
    display_name: "Parts"
  - name: Parts to Expedite
    expr: COUNT(1) FILTER (WHERE signal = 'EXPEDITE')
    comment: "Parts with on-hand stock already below safety stock."
    display_name: "Parts to expedite"
    synonyms: ["urgent parts", "critical parts"]
  - name: Parts to Order
    expr: COUNT(1) FILTER (WHERE signal = 'ORDER')
    comment: "Parts whose inventory position is below the reorder point."
    display_name: "Parts to order"
  - name: Stockout Risk Parts
    expr: COUNT(1) FILTER (WHERE stockout_before_next_receipt)
    comment: "Parts that run out before an order placed today could arrive."
    display_name: "Stockout risk parts"
    synonyms: ["at risk of stockout", "shortages"]
  - name: Suggested Order Value
    expr: SUM(suggested_order_value)
    comment: "Value of recommended orders in USD."
    display_name: "Suggested order value"
    synonyms: ["buy value", "open to buy"]
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
  - name: Inventory Value
    expr: SUM(on_hand_qty * unit_cost)
    comment: "Value of stock on hand in USD."
    display_name: "Inventory value"
    synonyms: ["stock value", "on hand value"]
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
  - name: Daily Usage Value
    expr: SUM(daily_run_rate * unit_cost)
    comment: "Forecast daily consumption in USD (DRR x unit cost)."
    display_name: "Daily usage value"
    synonyms: ["daily run rate value", "drr value"]
  - name: Days of Cover
    expr: "MEASURE(`Inventory Value`) / MEASURE(`Daily Usage Value`)"
    comment: "How many days on-hand stock lasts at the forecast run rate, value-weighted."
    display_name: "Days of cover"
    synonyms: ["days on hand", "DOH", "days of supply"]
    format:
      type: number
      decimal_places: {type: max, places: 1}
$$;

CREATE OR REPLACE VIEW workspace.ev_supply_chain.sourcing_risk_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: workspace.ev_supply_chain.gold_single_source_risk
comment: "Single-sourced parts (one supplier gets 80 percent or more of the spend) and how risky that dependence is."
dimensions:
  - name: Component Category
    expr: component_category
    comment: "Part category."
    display_name: "Component category"
    synonyms: ["category"]
  - name: Supplier
    expr: top_supplier_name
    comment: "The supplier the part depends on."
    display_name: "Supplier"
    synonyms: ["vendor"]
  - name: Supplier Country
    expr: top_supplier_country
    comment: "Country of that supplier."
    display_name: "Supplier country"
    synonyms: ["country"]
  - name: Supplier Tier
    expr: top_supplier_tier
    comment: "Reliability tier of that supplier."
    display_name: "Supplier tier"
  - name: Is High Risk
    expr: is_high_risk
    comment: "True when the supplier is tier C."
    display_name: "High risk"
measures:
  - name: Single-Sourced Parts
    expr: COUNT(1)
    comment: "Parts with one supplier at 80 percent or more of spend."
    display_name: "Single-sourced parts"
    synonyms: ["sole source parts", "single source"]
  - name: High-Risk Parts
    expr: COUNT(1) FILTER (WHERE is_high_risk)
    comment: "Single-sourced parts that depend on a tier C supplier."
    display_name: "High-risk parts"
  - name: Spend on Single-Sourced Parts
    expr: SUM(total_spend)
    comment: "Total spend on single-sourced parts, USD."
    display_name: "Single-sourced spend"
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
  - name: Spend at Risk
    expr: SUM(total_spend) FILTER (WHERE is_high_risk)
    comment: "Spend on high-risk parts, USD."
    display_name: "Spend at risk"
    synonyms: ["risky spend", "exposure"]
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
$$;
