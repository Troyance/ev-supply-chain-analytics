{{ config(materialized='metric_view', databricks_tags={'layer': 'gold', 'domain': 'vehicle_quality', 'certified': 'true', 'model_type': 'metric_view', 'source': 'nhtsa_public'}) }}
version: 1.1
source: {{ ref('fct_recall_campaign') }}
filter: source.involves_ev
comment: "Real public NHTSA data: US recall campaigns involving battery-electric vehicles, one row per campaign. Use for EV recall trends by year, component and manufacturer, and how many are fixed over the air."
dimensions:
  - name: Report Year
    expr: YEAR(source.report_date)
    comment: "Year the manufacturer reported the defect to NHTSA."
    display_name: "Report year"
    synonyms: ["year"]
  - name: Component Category
    expr: source.primary_component_category
    comment: "Main component affected: Software and electronics, Battery and high voltage, Body and interior, Restraints, Steering and suspension, Brakes, Drivetrain, Wiring and harnesses, Other."
    display_name: "Component"
    synonyms: ["component", "system"]
  - name: Manufacturer
    expr: source.manufacturer
    comment: "Company that filed the recall."
    display_name: "Manufacturer"
    synonyms: ["oem", "automaker"]
  - name: Is EV Only
    expr: source.is_ev_only
    comment: "True when every vehicle in the campaign is battery-electric. Only then does units affected mean EV units."
    display_name: "EV-only campaign"
  - name: Initiated By
    expr: source.initiated_by
    comment: "MFR (manufacturer), ODI or OVSC (NHTSA)."
    display_name: "Initiated by"
measures:
  - name: Recall Campaigns
    expr: COUNT(1)
    comment: "Recall campaigns involving at least one EV."
    display_name: "Recall campaigns"
    synonyms: ["recalls"]
  - name: OTA Campaigns
    expr: COUNT(1) FILTER (WHERE source.is_ota_remedy)
    comment: "Campaigns whose remedy is an over-the-air software update (read from the remedy text)."
    display_name: "Over-the-air fixes"
  - name: OTA Share
    expr: "MEASURE(`OTA Campaigns`) / MEASURE(`Recall Campaigns`)"
    comment: "Share of EV recall campaigns fixed over the air."
    display_name: "Over-the-air share"
    synonyms: ["ota rate"]
    format:
      type: percentage
      decimal_places: {type: max, places: 1}
  - name: EV Units Affected
    expr: SUM(source.units_affected) FILTER (WHERE source.is_ev_only)
    comment: "Vehicles affected, EV-only campaigns. Mixed campaigns are left out because their units include petrol vehicles."
    display_name: "EV units affected"
    format:
      type: number
      abbreviation: compact
      decimal_places: {type: max, places: 1}
  - name: Do Not Drive Campaigns
    expr: COUNT(1) FILTER (WHERE source.do_not_drive)
    comment: "Campaigns where owners were told not to drive the vehicle."
    display_name: "Do-not-drive recalls"
