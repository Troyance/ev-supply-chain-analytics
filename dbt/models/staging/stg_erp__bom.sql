select
    model_id                       as vehicle_model_id,
    cast(part_id as bigint)        as part_id,
    cast(qty_per_vehicle as int)   as qty_per_vehicle
from {{ source('erp', 'erp_bom') }}
