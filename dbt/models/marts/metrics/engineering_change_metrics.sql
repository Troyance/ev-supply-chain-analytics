{{ config(materialized='metric_view', databricks_tags={'layer': 'gold', 'domain': 'product_development', 'certified': 'true', 'model_type': 'metric_view'}) }}
version: 1.1
source: {{ ref('fct_engineering_change') }}
comment: "Engineering changes (ECOs), one row per change. Use for change volume, cycle time by stage, open backlog and where changes come from."
joins:
  - name: dim_program
    source: {{ ref('dim_program') }}
    'on': source.program_key = dim_program.program_key
dimensions:
  - name: Program
    expr: dim_program.program_name
    comment: "Pickup launch, SUV launch, Delivery van launch or SUV plant 2 launch."
    display_name: "Program"
  - name: Vehicle Model
    expr: dim_program.vehicle_model_id
    comment: "PU, SUV or VAN."
    display_name: "Vehicle model"
  - name: Raised In Phase
    expr: source.raised_in_phase
    comment: "EP, VP, PPV or Production - when the change was raised. Later is more expensive."
    display_name: "Raised in phase"
    synonyms: ["phase", "build phase"]
  - name: Reason
    expr: source.reason
    comment: "Quality issue, Cost reduction, Supplier change, Design improvement or Regulatory."
    display_name: "Reason"
    synonyms: ["change reason", "driver"]
  - name: Status
    expr: source.status
    comment: "Draft, In review, Approved, Implemented or Rejected."
    display_name: "Status"
  - name: Priority
    expr: source.priority
    comment: "High, Medium or Low."
    display_name: "Priority"
  - name: Component Category
    expr: source.component_category
    comment: "Category of the parts being changed."
    display_name: "Component category"
    synonyms: ["system", "category"]
  - name: Created Month
    expr: "DATE_TRUNC('MONTH', source.created_date)"
    comment: "Month the change was created."
    display_name: "Created month"
measures:
  - name: Changes
    expr: COUNT(1)
    comment: "Engineering changes."
    display_name: "Changes"
    synonyms: ["ECOs", "change orders", "ECNs"]
  - name: Open Changes
    expr: COUNT(1) FILTER (WHERE source.is_open)
    comment: "Changes not yet implemented or rejected."
    display_name: "Open changes"
    synonyms: ["backlog"]
  - name: Implemented Changes
    expr: COUNT(1) FILTER (WHERE source.status = 'Implemented')
    comment: "Changes in the BOM."
    display_name: "Implemented changes"
  - name: Avg Cycle Time Days
    expr: AVG(source.cycle_time_days)
    comment: "Average days from created to implemented, implemented changes only."
    display_name: "Avg cycle time (days)"
    synonyms: ["cycle time", "lead time to implement"]
  - name: Avg Days In Review
    expr: AVG(source.days_in_review)
    comment: "Average days from submitted to reviewed."
    display_name: "Avg days in review"
  - name: Avg Days To Approve
    expr: AVG(source.days_to_approve)
    comment: "Average days from reviewed to approved."
    display_name: "Avg days to approve"
  - name: Avg Days To Implement
    expr: AVG(source.days_to_implement)
    comment: "Average days from approved to implemented - mostly tooling and supplier lead time."
    display_name: "Avg days to implement"
