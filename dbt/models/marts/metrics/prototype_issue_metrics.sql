{{ config(materialized='metric_view', databricks_tags={'layer': 'gold', 'domain': 'product_development', 'certified': 'true', 'model_type': 'metric_view'}) }}
version: 1.1
source: {{ ref('fct_issue') }}
comment: "Issues found in prototype builds and production, one row per issue. Use for issue volume by phase and severity, time to close, and how many issues turn into engineering changes."
joins:
  - name: dim_program
    source: {{ ref('dim_program') }}
    'on': source.program_key = dim_program.program_key
dimensions:
  - name: Program
    expr: dim_program.program_name
    comment: "Vehicle program."
    display_name: "Program"
  - name: Found Phase
    expr: source.found_phase
    comment: "EP, VP, PPV or Production."
    display_name: "Found in phase"
    synonyms: ["phase", "build phase"]
  - name: Severity
    expr: source.severity
    comment: "S1 stop-ship, S2 major, S3 minor, S4 cosmetic."
    display_name: "Severity"
  - name: Root Cause
    expr: source.root_cause
    comment: "Design, Supplier quality, Assembly process, Software, or Test or measurement."
    display_name: "Root cause"
  - name: Component Category
    expr: source.component_category
    comment: "Category of the part with the issue."
    display_name: "Component category"
    synonyms: ["system", "category"]
  - name: Found Month
    expr: "DATE_TRUNC('MONTH', source.found_date)"
    comment: "Month the issue was found."
    display_name: "Found month"
measures:
  - name: Issues
    expr: COUNT(1)
    comment: "Issues found."
    display_name: "Issues"
    synonyms: ["defects", "problems"]
  - name: Open Issues
    expr: COUNT(1) FILTER (WHERE source.is_open)
    comment: "Issues not yet closed."
    display_name: "Open issues"
  - name: Avg Days To Close
    expr: AVG(source.days_to_close)
    comment: "Average days from found to closed, closed issues only."
    display_name: "Avg days to close"
  - name: Issues With ECO
    expr: COUNT(1) FILTER (WHERE source.raised_eco)
    comment: "Issues that led to an engineering change."
    display_name: "Issues raising a change"
  - name: ECO Conversion Rate
    expr: "MEASURE(`Issues With ECO`) / MEASURE(`Issues`)"
    comment: "Share of issues that needed an engineering change to fix."
    display_name: "Issue to change rate"
    format:
      type: percentage
      decimal_places: {type: max, places: 1}
