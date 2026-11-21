{{ config(tags=["simulation"]) }}
-- one row per site x part x policy. what each ordering policy cost over the simulated year.
-- holding 25%/yr of inventory value. ordering $50 per jit release, $150 local, $1,200 overseas.
-- shortage: plants $400 per missing part per vehicle (rework), service dc $25 + unit cost per backorder.
with sim as (
    select
        policy_key, site_key, part_key, site_id, part_id,
        sum(demand_units)                                       as demand_units,
        sum(short_units)                                        as short_units,
        sum(case when short_units > 0 then 1 else 0 end)        as stockout_days,
        sum(case when ordered_units > 0 then 1 else 0 end)      as orders_placed,
        avg(on_hand_units)                                      as avg_on_hand_units,
        count(*)                                                as days_simulated
    from {{ ref('fct_policy_simulation') }}
    group by all
),

qty_per_vehicle as (
    select part_key, avg(qty_per_vehicle) as qty_per_vehicle
    from {{ ref('bridge_vehicle_model_part') }}
    group by part_key
),

primary_source as (
    select b.part_key, s.region as supplier_region
    from {{ ref('bridge_part_supplier') }} b
    join {{ ref('dim_supplier') }} s using (supplier_key)
    where b.sourcing_role = 'primary'
),

costed as (
    select
        sim.*,
        pol.policy_id,
        pol.policy_name,
        p.part_number,
        p.part_name,
        p.component_category,
        p.buffer_class,
        ps.supplier_region,
        sim.avg_on_hand_units * p.standard_cost                                    as avg_inventory_value,
        sim.avg_on_hand_units * p.standard_cost * 0.25 * sim.days_simulated / 365   as holding_cost,
        sim.orders_placed * case when p.buffer_class = 'JIT' then 50
                                 when ps.supplier_region != 'America' then 1200 else 150 end as ordering_cost,
        sim.short_units * case when sim.site_id = 'SDC1' then 25 + p.standard_cost
                               else 400 / coalesce(q.qty_per_vehicle, 1) end        as shortage_cost
    from sim
    join {{ ref('dim_policy') }} pol using (policy_key)
    join {{ ref('dim_part') }} p using (part_key)
    left join primary_source ps using (part_key)
    left join qty_per_vehicle q using (part_key)
)

select
    policy_key,
    site_key,
    part_key,
    policy_id,
    policy_name,
    site_id,
    part_id,
    part_number,
    part_name,
    component_category,
    buffer_class,
    supplier_region,
    cast(demand_units as decimal(18, 2))                                        as demand_units,
    cast(short_units as decimal(18, 2))                                         as short_units,
    stockout_days,
    stockout_days > 0                                                           as had_stockout,
    orders_placed,
    cast(avg_inventory_value as decimal(18, 2))                                 as avg_inventory_value,
    cast(holding_cost as decimal(18, 2))                                        as holding_cost,
    cast(ordering_cost as decimal(18, 2))                                       as ordering_cost,
    cast(shortage_cost as decimal(18, 2))                                       as shortage_cost,
    cast(holding_cost + ordering_cost + shortage_cost as decimal(18, 2))        as total_cost
from costed
