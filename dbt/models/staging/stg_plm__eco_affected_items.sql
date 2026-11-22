select eco_id, model_id as vehicle_model_id, cast(part_id as bigint) as part_id, change_type
from {{ source('plm', 'plm_eco_affected_items') }}
