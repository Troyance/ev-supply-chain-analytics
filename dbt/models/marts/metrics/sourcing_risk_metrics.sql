{{ config(materialized='metric_view') }}
version: 1.1
source: {{ ref('mart_sourcing_risk') }}
comment: "One row per part. How dependent we are on single suppliers and how reliable they are. Most auto parts are single-sourced on purpose, so focus on High-Risk Parts (single-sourced from a tier C supplier)."
dimensions:
  - name: Component Category
    expr: component_category
    comment: "Part category."
    display_name: "Component category"
    synonyms: ["category"]
  - name: Supplier
    expr: top_supplier_name
    comment: "The supplier most of the part's spend goes to."
    display_name: "Supplier"
    synonyms: ["vendor"]
  - name: Supplier Country
    expr: top_supplier_country
    comment: "Country of that supplier."
    display_name: "Supplier country"
  - name: Supplier Region
    expr: top_supplier_region
    comment: "Region of that supplier."
    display_name: "Supplier region"
    synonyms: ["region"]
  - name: Supplier Tier
    expr: top_supplier_tier
    comment: "Current tier of that supplier."
    display_name: "Supplier tier"
  - name: Is Single Sourced
    expr: is_single_sourced
    comment: "Only one supplier set up for the part."
    display_name: "Single-sourced"
  - name: Is High Risk
    expr: is_high_risk
    comment: "Single-sourced from a tier C supplier."
    display_name: "High risk"
  - name: Part
    expr: "concat(part_number, ' ', part_name)"
    comment: "Part number and name."
    display_name: "Part"
measures:
  - name: Parts
    expr: COUNT(1)
    comment: "Parts bought."
    display_name: "Parts"
  - name: Single-Sourced Parts
    expr: COUNT(1) FILTER (WHERE is_single_sourced)
    comment: "Parts with one approved supplier."
    display_name: "Single-sourced parts"
    synonyms: ["sole source parts"]
  - name: High-Risk Parts
    expr: COUNT(1) FILTER (WHERE is_high_risk)
    comment: "Single-sourced parts that depend on a tier C supplier."
    display_name: "High-risk parts"
  - name: Spend
    expr: SUM(total_spend)
    comment: "Spend on these parts, USD."
    display_name: "Spend"
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
  - name: Spend at Risk
    expr: SUM(total_spend) FILTER (WHERE is_high_risk)
    comment: "Spend on high-risk parts, USD."
    display_name: "Spend at risk"
    synonyms: ["exposure", "risky spend"]
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places: {type: max, places: 1}
