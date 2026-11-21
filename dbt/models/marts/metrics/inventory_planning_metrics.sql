{{ config(materialized='metric_view') }}
version: 1.1
source: {{ ref('mart_buy_signals') }}
comment: "Current inventory position and replenishment recommendations, one row per site and part. Use for what to order or expedite, inventory value and days of cover."
dimensions:
  - name: Site
    expr: site_id
    comment: "PLT1 (Plant 1 Midwest), PLT2 (Plant 2 Southeast) or SDC1 (Service Parts DC)."
    display_name: "Site"
    synonyms: ["plant", "warehouse", "location"]
  - name: Signal
    expr: signal
    comment: "EXPEDITE (on hand below safety stock), ORDER (below reorder point), OK, EXCESS (over twice the max), NO DEMAND."
    display_name: "Signal"
    synonyms: ["action", "buy signal", "recommendation"]
  - name: Buffer Class
    expr: buffer_class
    comment: "JIT, KANBAN, STANDARD or STRATEGIC."
    display_name: "Buffer class"
    synonyms: ["replenishment strategy"]
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
    comment: "Main supplier."
    display_name: "Supplier"
  - name: Supplier Region
    expr: supplier_region
    comment: "Region the main supplier ships from."
    display_name: "Supplier region"
  - name: Supplier Tier
    expr: supplier_tier
    comment: "Main supplier's current tier."
    display_name: "Supplier tier"
  - name: Demand Type
    expr: demand_type
    comment: "dependent (plant, from builds x BOM) or independent (service parts)."
    display_name: "Demand type"
measures:
  - name: Parts
    expr: COUNT(1)
    comment: "Site and part combinations."
    display_name: "Parts"
  - name: Parts to Expedite
    expr: COUNT(1) FILTER (WHERE signal = 'EXPEDITE')
    comment: "On hand already below safety stock."
    display_name: "Parts to expedite"
    synonyms: ["urgent parts"]
  - name: Parts to Order
    expr: COUNT(1) FILTER (WHERE signal = 'ORDER')
    comment: "Inventory position below the reorder point."
    display_name: "Parts to order"
  - name: Stockout Risk Parts
    expr: COUNT(1) FILTER (WHERE stockout_before_next_receipt)
    comment: "Parts that run out before an order placed today could arrive."
    display_name: "Stockout risk parts"
    synonyms: ["shortages", "at risk"]
  - name: Suggested Order Value
    expr: SUM(suggested_order_value)
    comment: "Value of recommended orders, USD."
    display_name: "Suggested order value"
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
  - name: Inventory Value
    expr: SUM(on_hand_qty * unit_price)
    comment: "Value of stock on hand, USD."
    display_name: "Inventory value"
    synonyms: ["stock value"]
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
  - name: Daily Usage Value
    expr: SUM(daily_run_rate * unit_price)
    comment: "Forecast daily consumption, USD (DRR x unit price)."
    display_name: "Daily usage value"
    synonyms: ["drr value"]
  - name: Days of Cover
    expr: "MEASURE(`Inventory Value`) / MEASURE(`Daily Usage Value`)"
    comment: "Days on-hand stock lasts at the forecast run rate, value weighted."
    display_name: "Days of cover"
    synonyms: ["days on hand", "DOH", "days of supply"]
    format:
      type: number
      decimal_places: {type: max, places: 1}
