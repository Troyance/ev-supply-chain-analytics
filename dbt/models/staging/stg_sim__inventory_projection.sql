{{ config(tags=['simulation']) }}
select
    site_id,
    cast(part_id as bigint)   as part_id,
    day                       as projection_date,
    day_index,
    demand                    as demand_units,
    receipts                  as received_units,
    order_placed              as ordered_units,
    on_hand                   as on_hand_units,
    inventory_position        as inventory_position_units,
    unmet_demand              as short_units,
    safety_stock              as safety_stock_units,
    reorder_point             as reorder_point_units
from {{ source('sim', 'sim_inventory_projection') }}
