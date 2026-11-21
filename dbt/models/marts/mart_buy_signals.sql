-- one row per site x part: run rate, safety stock, reorder point, and what to do today
with plan as (
    select
        site_key, part_key, site_id, part_id,
        any_value(demand_type)                                         as demand_type,
        sum(demand_p50) / (13 * 7)                                     as drr,
        -- (p90 - p50) / 1.28 is about one std dev of weekly demand, spread over 7 days
        sqrt(avg(pow((demand_p90 - demand_p50) / 1.2816, 2)) / 7)      as demand_sd_daily
    from {{ ref('mart_part_demand_plan') }}
    group by all
),

latest_snapshot as (
    select * from {{ ref('fct_inventory_snapshot') }}
    where snapshot_date = (select max(snapshot_date) from {{ ref('fct_inventory_snapshot') }})
),

primary_source as (
    select b.part_key, b.supplier_key, b.unit_price, s.supplier_name, s.region as supplier_region, s.current_tier
    from {{ ref('bridge_part_supplier') }} b
    join {{ ref('dim_supplier') }} s using (supplier_key)
    where b.sourcing_role = 'primary'
),

params as (
    select
        pl.*,
        p.part_number, p.part_name, p.component_category, p.buffer_class, p.planning_lead_time_days,
        p.lead_time_stddev_days, p.review_period_days, p.service_level_z, p.pack_size, p.moq,
        ps.supplier_key, ps.supplier_name, ps.supplier_region, ps.current_tier as supplier_tier, ps.unit_price,
        coalesce(i.snapshot_date, (select max(snapshot_date) from latest_snapshot))  as snapshot_date,
        coalesce(i.on_hand_qty, 0)                                                   as on_hand_qty,
        coalesce(i.on_order_qty, 0)                                                  as on_order_qty,
        case p.buffer_class
            when 'JIT'    then pl.drr * 0.5
            when 'KANBAN' then pl.drr * 3
            else p.service_level_z * sqrt(p.planning_lead_time_days * pow(pl.demand_sd_daily, 2)
                                          + pow(pl.drr * p.lead_time_stddev_days, 2))
        end                                                                          as safety_stock
    from plan pl
    join {{ ref('dim_part') }} p using (part_key)
    join primary_source ps using (part_key)
    left join latest_snapshot i using (site_key, part_key)
),

levels as (
    select
        *,
        drr * (planning_lead_time_days + review_period_days) + safety_stock         as reorder_point,
        drr * (planning_lead_time_days + 2 * review_period_days) + safety_stock     as order_up_to,
        on_hand_qty + on_order_qty                                                  as inventory_position,
        on_hand_qty / nullif(drr, 0)                                                as days_of_cover
    from params
),

signals as (
    select
        *,
        case
            when drr = 0                               then 'NO DEMAND'
            when on_hand_qty < safety_stock            then 'EXPEDITE'
            when inventory_position < reorder_point    then 'ORDER'
            when inventory_position > 2 * order_up_to  then 'EXCESS'
            else 'OK'
        end as signal
    from levels
)

select
    snapshot_date,
    site_key,
    part_key,
    supplier_key,
    site_id,
    part_id,
    part_number,
    part_name,
    component_category,
    demand_type,
    buffer_class,
    supplier_name,
    supplier_region,
    supplier_tier,
    cast(drr as double)                                                    as daily_run_rate,
    cast(demand_sd_daily as double)                                        as demand_sd_daily,
    planning_lead_time_days,
    lead_time_stddev_days,
    review_period_days,
    cast(round(safety_stock) as double)                                    as safety_stock,
    cast(safety_stock / nullif(drr, 0) as double)                          as safety_stock_days,
    cast(round(reorder_point) as double)                                   as reorder_point,
    cast(round(order_up_to) as double)                                     as order_up_to,
    on_hand_qty,
    on_order_qty,
    inventory_position,
    cast(days_of_cover as double)                                          as days_of_cover,
    coalesce(days_of_cover < planning_lead_time_days, false)               as stockout_before_next_receipt,
    signal,
    -- order up to the max, at least the moq, in whole packs
    cast(case when signal in ('ORDER', 'EXPEDITE')
              then greatest(ceil((order_up_to - inventory_position) / pack_size) * pack_size, moq) else 0 end as bigint)
                                                                           as suggested_order_qty,
    cast(case when signal in ('ORDER', 'EXPEDITE')
              then greatest(ceil((order_up_to - inventory_position) / pack_size) * pack_size, moq) * unit_price else 0 end
         as decimal(18, 2))                                                as suggested_order_value,
    date_add(snapshot_date, planning_lead_time_days)                       as expected_receipt_date,
    unit_price,
    pack_size,
    moq
from signals
