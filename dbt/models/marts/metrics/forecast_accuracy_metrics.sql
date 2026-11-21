{{ config(materialized='metric_view') }}
version: 1.1
source: {{ ref('mart_forecast_accuracy') }}
comment: "Backtest forecast accuracy for vehicle builds and service parts: LightGBM against a naive baseline."
dimensions:
  - name: Forecast Type
    expr: forecast_type
    comment: "vehicle_builds (weekly units per plant and model) or service_parts (weekly units per part at the DC)."
    display_name: "Forecast type"
  - name: Model
    expr: model
    comment: "LightGBM or Naive baseline."
    display_name: "Model"
    synonyms: ["method"]
  - name: Horizon Weeks
    expr: horizon_weeks
    comment: "Weeks ahead the forecast was made, 1-13."
    display_name: "Weeks ahead"
    synonyms: ["horizon", "lag"]
  - name: Target Week
    expr: target_week
    comment: "Week being forecast."
    display_name: "Target week"
  - name: Site
    expr: site_id
    comment: "PLT1, PLT2 or SDC1."
    display_name: "Site"
  - name: Vehicle Model
    expr: vehicle_model_id
    comment: "PU, SUV or VAN for build forecasts."
    display_name: "Vehicle model"
  - name: Component Category
    expr: component_category
    comment: "Part category for service parts, Vehicle for builds."
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
    comment: "Sum of absolute errors, units."
    display_name: "Absolute error"
  - name: WAPE
    expr: "MEASURE(`Absolute Error`) / MEASURE(`Actual Units`)"
    comment: "Weighted absolute percent error, 0-1. Lower is better."
    display_name: "WAPE"
    synonyms: ["forecast error", "accuracy"]
    format:
      type: percentage
      decimal_places: {type: max, places: 1}
  - name: Bias
    expr: "(MEASURE(`Forecast Units`) - MEASURE(`Actual Units`)) / MEASURE(`Actual Units`)"
    comment: "Positive means forecasts run high."
    display_name: "Bias"
    format:
      type: percentage
      decimal_places: {type: max, places: 1}
  - name: Interval Coverage
    expr: AVG(CASE WHEN within_interval THEN 1.0 WHEN NOT within_interval THEN 0.0 END)
    comment: "Share of actuals inside p10-p90. About 0.8 means well calibrated. Null for the baseline."
    display_name: "p10-p90 coverage"
    format:
      type: percentage
      decimal_places: {type: max, places: 1}
