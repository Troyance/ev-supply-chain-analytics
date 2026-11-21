select
    snapshot_date,
    site_id,
    cast(part_id as bigint)        as part_id,
    cast(on_hand_qty as bigint)    as on_hand_qty,
    cast(on_order_qty as bigint)   as on_order_qty
from {{ source('wms', 'fct_inventory_snapshot') }}
