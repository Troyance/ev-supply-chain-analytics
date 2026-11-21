-- the bom. vehicle models and parts are many to many, so like sourcing it's a bridge
select
    {{ dbt_utils.generate_surrogate_key(['vehicle_model_id']) }}  as vehicle_model_key,
    {{ dbt_utils.generate_surrogate_key(['part_id']) }}           as part_key,
    vehicle_model_id,
    part_id,
    qty_per_vehicle
from {{ ref('stg_erp__bom') }}
