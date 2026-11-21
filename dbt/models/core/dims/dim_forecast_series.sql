-- one row per thing we forecast. replaces the "model_id or part_id, the other one null" pattern with one key
with series as (
    select distinct forecast_type, site_id, vehicle_model_id, part_id
    from {{ ref('stg_ml__forecasts') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['forecast_type', 'site_id', 'vehicle_model_id', 'part_id']) }} as series_key,
    forecast_type,
    site_id,
    vehicle_model_id,
    part_id,
    {{ dbt_utils.generate_surrogate_key(['site_id']) }}                                                 as site_key,
    case when vehicle_model_id is not null then {{ dbt_utils.generate_surrogate_key(['vehicle_model_id']) }} end as vehicle_model_key,
    case when part_id is not null then {{ dbt_utils.generate_surrogate_key(['part_id']) }} end          as part_key,
    case when forecast_type = 'vehicle_builds' then concat(site_id, ' ', vehicle_model_id)
         else concat(site_id, ' part ', cast(part_id as string)) end                                    as series_name
from series
