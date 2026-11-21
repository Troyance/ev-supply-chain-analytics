{{ config(materialized='view', tags=['simulation']) }}
-- daily stock per part under each policy, with readable labels. a view: the fact is already at this grain
select
    concat(f.site_id, ' ', p.part_number, ' ', p.part_name)   as part_label,
    f.site_id,
    f.part_id,
    pol.policy_name,
    f.sim_date,
    f.demand_units,
    f.on_hand_units,
    f.short_units
from {{ ref('fct_policy_simulation') }} f
join {{ ref('dim_part') }} p using (part_key)
join {{ ref('dim_policy') }} pol using (policy_key)
