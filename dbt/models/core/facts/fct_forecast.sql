-- grain: forecast run x series x origin week x target week. append-only: a forecast is never changed after it's made,
-- so each run only adds the runs it hasn't seen yet.
{{ config(materialized='incremental', incremental_strategy='append', on_schema_change='fail') }}

select
    {{ dbt_utils.generate_surrogate_key(['run_id']) }}                                                   as forecast_run_key,
    {{ dbt_utils.generate_surrogate_key(['forecast_type', 'site_id', 'vehicle_model_id', 'part_id']) }}  as series_key,
    {{ date_key('origin_week') }}                                                                        as origin_week_date_key,
    {{ date_key('target_week') }}                                                                        as target_week_date_key,
    run_id,
    origin_week,
    target_week,
    horizon_weeks,
    cast(forecast_p10 as decimal(18, 2))   as forecast_p10,
    cast(forecast_p50 as decimal(18, 2))   as forecast_p50,
    cast(forecast_p90 as decimal(18, 2))   as forecast_p90
from {{ ref('stg_ml__forecasts') }}
{% if is_incremental() %}
where run_id not in (select distinct run_id from {{ this }})
{% endif %}
