select
    cast(parent_item_id as bigint)   as parent_item_id,
    cast(child_item_id as bigint)    as child_item_id,
    cast(qty as int)                 as qty,
    effective_from,
    effective_to,
    eco_id
from {{ source('plm', 'plm_bom_lines') }}
