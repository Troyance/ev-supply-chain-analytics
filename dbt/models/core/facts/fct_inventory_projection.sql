{{ config(tags=["simulation"]) }}
-- grain: site x part x day. today's stock played forward 13 weeks under the current policy
select
    {{ dbt_utils.generate_surrogate_key(['site_id']) }}   as site_key,
    {{ dbt_utils.generate_surrogate_key(['part_id']) }}   as part_key,
    {{ date_key('projection_date') }}                      as projection_date_key,
    site_id,
    part_id,
    projection_date,
    day_index,
    cast(demand_units as decimal(18, 2))                  as demand_units,
    cast(received_units as decimal(18, 2))                as received_units,
    cast(ordered_units as decimal(18, 2))                 as ordered_units,
    cast(on_hand_units as decimal(18, 2))                 as on_hand_units,
    cast(inventory_position_units as decimal(18, 2))      as inventory_position_units,
    cast(short_units as decimal(18, 2))                   as short_units,
    cast(safety_stock_units as decimal(18, 2))            as safety_stock_units,
    cast(reorder_point_units as decimal(18, 2))           as reorder_point_units
from {{ ref('stg_sim__inventory_projection') }}
