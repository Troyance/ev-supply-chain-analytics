{{ config(materialized='metric_view') }}
version: 1.1
source: {{ ref('fct_po_deliveries') }}
comment: "Supplier delivery performance and spend, one row per PO delivery line. Supplier Tier is point-in-time (the tier the supplier had when the goods arrived). Use for on-time delivery, lead time, lateness and spend by supplier, region, category, plant or month."
joins:
  - name: dim_supplier
    source: {{ ref('dim_supplier') }}
    'on': source.supplier_key = dim_supplier.supplier_key
  - name: dim_tier
    source: {{ ref('dim_supplier_tier_history') }}
    'on': source.supplier_tier_key = dim_tier.supplier_tier_key
  - name: dim_scorecard
    source: {{ ref('mart_supplier_scorecard') }}
    'on': source.supplier_key = dim_scorecard.supplier_key
  - name: dim_part
    source: {{ ref('dim_part') }}
    'on': source.part_key = dim_part.part_key
  - name: dim_received
    source: {{ ref('dim_date') }}
    'on': source.received_date_key = dim_received.date_key
dimensions:
  - name: Supplier
    expr: dim_supplier.supplier_name
    comment: "Supplier name."
    display_name: "Supplier"
    synonyms: ["vendor"]
  - name: Supplier Country
    expr: dim_supplier.country
    comment: "Country the supplier ships from."
    display_name: "Supplier country"
    synonyms: ["country", "origin"]
  - name: Supplier Region
    expr: dim_supplier.region
    comment: "America, Asia, Europe, Africa or Middle East."
    display_name: "Supplier region"
    synonyms: ["region", "sourcing region"]
  - name: Supplier Tier
    expr: dim_tier.tier
    comment: "Tier the supplier had when the delivery arrived (point in time): A 97%+ on time, B 90-97%, C under 90%, Unrated."
    display_name: "Supplier tier (at delivery)"
    synonyms: ["tier", "rating"]
  - name: Current Supplier Tier
    expr: dim_supplier.current_tier
    comment: "The supplier's tier today."
    display_name: "Current supplier tier"
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
    synonyms: ["part number", "sku", "component"]
  - name: Plant
    expr: source.site_id
    comment: "PLT1 (Midwest) or PLT2 (Southeast)."
    display_name: "Plant"
    synonyms: ["site", "factory"]
  - name: Sourcing Role
    expr: source.sourcing_role
    comment: "primary or secondary source."
    display_name: "Sourcing role"
  - name: Ship Mode
    expr: source.ship_mode
    comment: "Truck, Rail or Ship."
    display_name: "Ship mode"
  - name: Receipt Month
    expr: dim_received.month_start
    comment: "Month the goods arrived. Default for trends."
    display_name: "Receipt month"
    synonyms: ["month", "delivery month"]
  - name: Receipt Year
    expr: dim_received.calendar_year
    comment: "Year the goods arrived."
    display_name: "Receipt year"
    synonyms: ["year"]
  - name: Order Month
    expr: "DATE_TRUNC('MONTH', source.order_date)"
    comment: "Month the order was placed. Use this to see the chip shortage, which hit orders placed mid 2021 to end of 2022."
    display_name: "Order month"
measures:
  - name: Delivery Lines
    expr: COUNT(1)
    comment: "PO delivery lines received."
    display_name: "Delivery lines"
    synonyms: ["deliveries", "receipts"]
  - name: On-Time Lines
    expr: COUNT(1) FILTER (WHERE source.is_on_time)
    comment: "Lines received on or before the promised date."
    display_name: "On-time lines"
  - name: On-Time Rate
    expr: "MEASURE(`On-Time Lines`) / MEASURE(`Delivery Lines`)"
    comment: "Share of delivery lines received on time, 0-1 (OTD). Automakers expect 95%+."
    display_name: "On-time delivery"
    synonyms: ["OTD", "on time percent", "reliability"]
    format:
      type: percentage
      decimal_places: {type: max, places: 1}
  - name: Spend
    expr: SUM(source.net_amount)
    comment: "Net spend in USD."
    display_name: "Spend"
    synonyms: ["cost", "purchase value"]
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
  - name: Units Received
    expr: SUM(source.quantity)
    comment: "Units delivered."
    display_name: "Units received"
  - name: Avg Lead Time Days
    expr: AVG(source.lead_time_days)
    comment: "Average days from order to receipt."
    display_name: "Avg lead time (days)"
    synonyms: ["lead time"]
  - name: Avg Days Late When Late
    expr: AVG(source.days_late) FILTER (WHERE source.days_late > 0)
    comment: "For late lines only, average days past the promised date."
    display_name: "Avg days late (late lines)"
  - name: Active Suppliers
    expr: COUNT(DISTINCT source.supplier_key)
    comment: "Distinct suppliers with deliveries."
    display_name: "Active suppliers"
    synonyms: ["supplier count"]
