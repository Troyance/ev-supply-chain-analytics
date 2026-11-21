-- weekly builds per plant and vehicle model: actuals, plus the latest production forecast for the next 13 weeks
with latest_production as (
    select forecast_run_key from {{ ref('dim_forecast_run') }}
    where run_type = 'production' and forecast_type = 'vehicle_builds' and is_latest
)

select
    b.week_start,
    b.site_id,
    b.vehicle_model_id,
    concat(b.site_id, ' ', b.vehicle_model_id)            as production_line,
    'actual'                                              as row_type,
    cast(b.units_built as decimal(18, 2))                 as actual_units,
    cast(null as decimal(18, 2))                          as forecast_p10,
    cast(null as decimal(18, 2))                          as forecast_p50,
    cast(null as decimal(18, 2))                          as forecast_p90
from {{ ref('fct_vehicle_builds') }} b

union all

select
    f.target_week,
    s.site_id,
    s.vehicle_model_id,
    concat(s.site_id, ' ', s.vehicle_model_id),
    'forecast',
    null,
    f.forecast_p10,
    f.forecast_p50,
    f.forecast_p90
from {{ ref('fct_forecast') }} f
join latest_production using (forecast_run_key)
join {{ ref('dim_forecast_series') }} s using (series_key)
