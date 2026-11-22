{{ config(databricks_tags={"layer": "gold", "domain": "product_development", "certified": "true", "model_type": "mart"}) }}
-- where product development meets supply chain: parts with a pending engineering change that will replace or
-- remove them, next to today's buy signal and stock. ordering more of a part that's about to be superseded is how
-- plants end up with obsolete inventory.
with pending as (
    select
        b.part_key,
        b.vehicle_model_id,
        e.eco_id,
        e.status         as eco_status,
        e.reason         as eco_reason,
        b.change_type,
        e.created_date   as eco_created_date,
        e.approved_date  as eco_approved_date
    from {{ ref('bridge_eco_item') }} b
    join {{ ref('fct_engineering_change') }} e using (eco_key)
    where e.is_open and b.change_type in ('Replace part', 'Remove part')
)

select
    s.site_key,
    s.part_key,
    s.site_id,
    s.part_number,
    s.part_name,
    s.component_category,
    p.eco_id,
    p.eco_status,
    p.eco_reason,
    p.change_type,
    p.eco_created_date,
    p.eco_approved_date,
    s.signal,
    s.on_hand_qty,
    s.on_order_qty,
    s.suggested_order_qty,
    s.suggested_order_value,
    cast((s.on_hand_qty + s.on_order_qty) * s.unit_price as decimal(18, 2))   as stock_at_risk_value,
    case when s.signal in ('ORDER', 'EXPEDITE') then 'Hold order - part being superseded'
         else 'Run down stock before cut-in' end                               as recommended_action
from {{ ref('mart_buy_signals') }} s
join pending p using (part_key)
