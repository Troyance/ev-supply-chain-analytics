-- grain: week x site x vehicle model
select
    {{ date_key('week_start') }}                                           as week_date_key,
    {{ dbt_utils.generate_surrogate_key(['site_id']) }}                    as site_key,
    {{ dbt_utils.generate_surrogate_key(['vehicle_model_id']) }}           as vehicle_model_key,
    week_start,
    site_id,
    vehicle_model_id,
    units_built
from {{ ref('stg_erp__vehicle_builds') }}
