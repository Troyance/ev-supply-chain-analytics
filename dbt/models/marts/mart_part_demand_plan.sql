-- next 13 weeks of part demand per site, from the latest production forecasts.
-- plants: forecast builds x bom (dependent). service dc: the service parts forecast directly.
with latest_production as (
    select forecast_run_key
    from {{ ref('dim_forecast_run') }}
    where run_type = 'production' and is_latest
),

forecasts as (
    select f.*, s.forecast_type, s.site_key, s.site_id, s.vehicle_model_key, s.part_key as series_part_key
    from {{ ref('fct_forecast') }} f
    join latest_production using (forecast_run_key)
    join {{ ref('dim_forecast_series') }} s using (series_key)
)

select
    f.forecast_run_key,
    f.site_key,
    b.part_key,
    f.site_id,
    b.part_id,
    f.target_week,
    f.horizon_weeks,
    'dependent'                                                            as demand_type,
    -- adding p90s across vehicle models assumes they all run high together. conservative
    cast(sum(f.forecast_p10 * b.qty_per_vehicle) as decimal(18, 2))        as demand_p10,
    cast(sum(f.forecast_p50 * b.qty_per_vehicle) as decimal(18, 2))        as demand_p50,
    cast(sum(f.forecast_p90 * b.qty_per_vehicle) as decimal(18, 2))        as demand_p90
from forecasts f
join {{ ref('bridge_vehicle_model_part') }} b using (vehicle_model_key)
where f.forecast_type = 'vehicle_builds'
group by all

union all

select
    f.forecast_run_key, f.site_key, f.series_part_key, f.site_id, p.part_id, f.target_week, f.horizon_weeks,
    'independent', f.forecast_p10, f.forecast_p50, f.forecast_p90
from forecasts f
join {{ ref('dim_part') }} p on p.part_key = f.series_part_key
where f.forecast_type = 'service_parts'
