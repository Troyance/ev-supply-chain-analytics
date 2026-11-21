-- grain: policy x site x part x day
select
    {{ dbt_utils.generate_surrogate_key(['policy_id']) }}      as policy_key,
    {{ dbt_utils.generate_surrogate_key(['site_id']) }}        as site_key,
    {{ dbt_utils.generate_surrogate_key(['part_id']) }}        as part_key,
    {{ date_key('sim_date') }}                                  as sim_date_key,
    policy_id,
    site_id,
    part_id,
    sim_date,
    cast(demand_units as decimal(18, 2))     as demand_units,
    cast(on_hand_units as decimal(18, 2))    as on_hand_units,
    cast(received_units as decimal(18, 2))   as received_units,
    cast(ordered_units as decimal(18, 2))    as ordered_units,
    cast(short_units as decimal(18, 2))      as short_units
from {{ ref('stg_sim__policy_daily') }}
