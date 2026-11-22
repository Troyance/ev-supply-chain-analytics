-- which parts each engineering change touches, and how
select
    {{ dbt_utils.generate_surrogate_key(['eco_id']) }}             as eco_key,
    {{ dbt_utils.generate_surrogate_key(['part_id']) }}            as part_key,
    {{ dbt_utils.generate_surrogate_key(['vehicle_model_id']) }}   as vehicle_model_key,
    eco_id,
    part_id,
    vehicle_model_id,
    change_type
from {{ ref('stg_plm__eco_affected_items') }}
