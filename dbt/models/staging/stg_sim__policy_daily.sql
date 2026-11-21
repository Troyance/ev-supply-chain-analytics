select
    policy   as policy_id,
    site_id,
    cast(part_id as bigint)  as part_id,
    day      as sim_date,
    demand   as demand_units,
    on_hand  as on_hand_units,
    receipts as received_units,
    order_qty as ordered_units,
    unmet    as short_units
from {{ source('ml', 'gold_policy_simulation_daily') }}
