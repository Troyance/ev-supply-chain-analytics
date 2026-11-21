{{ config(tags=["simulation"]) }}
-- one row per site x part: how the next 13 weeks look from today's stock
select
    f.site_key,
    f.part_key,
    f.site_id,
    f.part_id,
    p.part_number,
    p.part_name,
    p.component_category,
    p.buffer_class,
    b.signal,
    b.supplier_name,
    b.supplier_tier,
    cast(min(f.on_hand_units) as decimal(18, 2))                                   as min_on_hand_units,
    sum(case when f.on_hand_units <= 0 then 1 else 0 end)                          as projected_stockout_days,
    min(case when f.on_hand_units <= 0 then f.projection_date end)                 as first_stockout_date,
    sum(case when f.on_hand_units < f.safety_stock_units then 1 else 0 end)        as days_below_safety_stock,
    sum(case when f.ordered_units > 0 then 1 else 0 end)                           as orders_placed,
    cast(sum(f.demand_units) as decimal(18, 2))                                    as demand_units,
    cast(sum(f.short_units) as decimal(18, 2))                                     as short_units,
    cast(avg(f.on_hand_units * p.standard_cost) as decimal(18, 2))                 as avg_inventory_value
from {{ ref('fct_inventory_projection') }} f
join {{ ref('dim_part') }} p using (part_key)
left join {{ ref('mart_buy_signals') }} b using (site_key, part_key)
group by all
