{{ config(materialized='metric_view', tags=['simulation']) }}
version: 1.1
source: {{ ref('mart_policy_comparison') }}
comment: "Ordering policies compared on the last 52 weeks of real demand, one row per site x part x policy. Lead with Stockout Rate (share of parts that ran out at least once); unit fill rate looks high because cheap high-volume parts dominate it."
dimensions:
  - name: Policy
    expr: policy_name
    comment: "Current, Lean, High service, EOQ, DDMRP, MRP (schedule)."
    display_name: "Policy"
    synonyms: ["ordering policy", "replenishment rule"]
  - name: Site
    expr: site_id
    comment: "PLT1, PLT2 or SDC1."
    display_name: "Site"
    synonyms: ["plant"]
  - name: Buffer Class
    expr: buffer_class
    comment: "JIT, KANBAN, STANDARD or STRATEGIC."
    display_name: "Buffer class"
  - name: Component Category
    expr: component_category
    comment: "Part category."
    display_name: "Component category"
  - name: Supplier Region
    expr: supplier_region
    comment: "Region of the main supplier."
    display_name: "Supplier region"
  - name: Part
    expr: "concat(part_number, ' ', part_name)"
    comment: "Part number and name."
    display_name: "Part"
measures:
  - name: Parts
    expr: COUNT(1)
    comment: "Site x part combinations simulated (per policy)."
    display_name: "Parts"
  - name: Parts with Stockout
    expr: COUNT(1) FILTER (WHERE had_stockout)
    comment: "Parts that ran short at least once."
    display_name: "Parts with a stockout"
  - name: Stockout Rate
    expr: "MEASURE(`Parts with Stockout`) / MEASURE(`Parts`)"
    comment: "Share of parts that ran short at least once in the year. The headline service KPI."
    display_name: "Stockout rate"
    synonyms: ["share of parts short", "stockouts"]
    format:
      type: percentage
      decimal_places: {type: max, places: 1}
  - name: Stockout Days
    expr: SUM(stockout_days)
    comment: "Total days with a shortage across parts."
    display_name: "Stockout days"
  - name: Unit Fill Rate
    expr: 1 - SUM(short_units) / SUM(demand_units)
    comment: "Share of units supplied from stock. Dominated by high-volume cheap parts, so it always looks high."
    display_name: "Unit fill rate"
    format:
      type: percentage
      decimal_places: {type: max, places: 2}
  - name: Avg Inventory Value
    expr: SUM(avg_inventory_value)
    comment: "Average cash tied up in stock, USD."
    display_name: "Avg inventory value"
    synonyms: ["cash in stock", "working capital"]
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
  - name: Holding Cost
    expr: SUM(holding_cost)
    comment: "25% a year of inventory value, USD."
    display_name: "Holding cost"
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
  - name: Ordering Cost
    expr: SUM(ordering_cost)
    comment: "Cost of placing orders, USD."
    display_name: "Ordering cost"
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
  - name: Shortage Cost
    expr: SUM(shortage_cost)
    comment: "Rework and backorder cost of shortages, USD."
    display_name: "Shortage cost"
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
  - name: Total Cost
    expr: SUM(total_cost)
    comment: "Holding + ordering + shortage, USD."
    display_name: "Total cost"
    synonyms: ["bottom line"]
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
