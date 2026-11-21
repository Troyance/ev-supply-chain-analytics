-- latest backtest of every model next to what actually happened, at the lowest grain.
-- no wape here on purpose: ratios get computed after aggregating, in the metric view.
with latest_backtests as (
    select forecast_run_key, model_name
    from {{ ref('dim_forecast_run') }}
    where run_type = 'backtest' and is_latest
),

forecasts as (
    select f.*, r.model_name, s.forecast_type, s.site_key, s.site_id, s.vehicle_model_key, s.vehicle_model_id,
           s.part_key, s.part_id
    from {{ ref('fct_forecast') }} f
    join latest_backtests r using (forecast_run_key)
    join {{ ref('dim_forecast_series') }} s using (series_key)
),

actuals as (
    select 'vehicle_builds' as forecast_type, week_start, site_key, vehicle_model_key, cast(null as string) as part_key,
           units_built as actual_units
    from {{ ref('fct_vehicle_builds') }}
    union all
    select 'service_parts', week_start, site_key, cast(null as string), part_key, units
    from {{ ref('fct_part_demand') }}
    where demand_type = 'independent'
)

select
    f.forecast_run_key,
    f.series_key,
    f.forecast_type,
    case when f.model_name = 'naive_mean' then 'Naive baseline' else 'LightGBM' end    as model,
    f.origin_week,
    f.target_week,
    f.horizon_weeks,
    f.site_id,
    f.vehicle_model_id,
    f.part_id,
    coalesce(p.component_category, 'Vehicle')                                         as component_category,
    f.forecast_p10,
    f.forecast_p50,
    f.forecast_p90,
    cast(a.actual_units as decimal(18, 2))                                            as actual_units,
    cast(abs(f.forecast_p50 - a.actual_units) as decimal(18, 2))                      as abs_error,
    cast(f.forecast_p50 - a.actual_units as decimal(18, 2))                           as error,
    case when f.forecast_p10 is not null
         then a.actual_units between f.forecast_p10 and f.forecast_p90 end            as within_interval
from forecasts f
join actuals a
    on a.forecast_type = f.forecast_type
    and a.week_start = f.target_week
    and a.site_key = f.site_key
    and a.vehicle_model_key <=> f.vehicle_model_key
    and a.part_key <=> f.part_key
left join {{ ref('dim_part') }} p on p.part_key = f.part_key
