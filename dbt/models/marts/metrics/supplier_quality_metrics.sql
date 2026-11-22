{{ config(materialized='metric_view', databricks_tags={'layer': 'gold', 'domain': 'supply_chain', 'certified': 'true', 'model_type': 'metric_view'}) }}
version: 1.1
source: {{ ref('mart_supplier_scorecard') }}
comment: "Supplier quality next to delivery reliability, one row per supplier. Quality issues are issues with root cause supplier quality on the supplier's parts, from prototype builds and production. PPM is always total issues per million total units, never an average of supplier PPMs."
dimensions:
  - name: Supplier
    expr: supplier_name
    comment: "Supplier name."
    display_name: "Supplier"
    synonyms: ["vendor"]
  - name: Supplier Tier
    expr: tier
    comment: "Delivery reliability tier: A 97%+ on time, B 90-97%, C under 90%."
    display_name: "Supplier tier"
    synonyms: ["tier"]
  - name: Supplier Region
    expr: region
    comment: "Region the supplier ships from."
    display_name: "Supplier region"
    synonyms: ["region"]
  - name: Supplier Country
    expr: country
    comment: "Country the supplier ships from."
    display_name: "Supplier country"
  - name: Is Declining Supplier
    expr: is_declining
    comment: "Getting at least 0.15 days later every month."
    display_name: "Declining supplier"
measures:
  - name: Suppliers
    expr: COUNT(1)
    comment: "Suppliers."
    display_name: "Suppliers"
  - name: Quality Issues
    expr: SUM(quality_issues)
    comment: "Issues caused by supplier quality."
    display_name: "Quality issues"
    synonyms: ["defects", "supplier defects"]
  - name: Severe Quality Issues
    expr: SUM(severe_quality_issues)
    comment: "Quality issues rated S1 (stop-ship) or S2 (major)."
    display_name: "Severe quality issues"
  - name: Units Delivered
    expr: SUM(units_delivered)
    comment: "Units delivered."
    display_name: "Units delivered"
  - name: Quality PPM
    expr: "MEASURE(`Quality Issues`) * 1000000 / MEASURE(`Units Delivered`)"
    comment: "Supplier-caused issue reports per million units delivered. Counts issue reports, not defective parts (one issue can cover many bad parts), so values sit far below industry part PPM - compare suppliers with each other, not with outside benchmarks."
    display_name: "Quality PPM"
    synonyms: ["ppm", "defect rate", "parts per million"]
    format:
      type: number
      decimal_places: {type: max, places: 2}
  - name: Quality Issues per Supplier
    expr: "MEASURE(`Quality Issues`) / MEASURE(`Suppliers`)"
    comment: "Average quality issues per supplier."
    display_name: "Quality issues per supplier"
    format:
      type: number
      decimal_places: {type: max, places: 2}
