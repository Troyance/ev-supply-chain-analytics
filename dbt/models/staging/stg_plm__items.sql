select cast(item_id as bigint) as item_id, item_number, item_name, item_type, component_category, lifecycle_status,
       cast(superseded_by_item_id as bigint) as superseded_by_item_id
from {{ source('plm', 'plm_items') }}
