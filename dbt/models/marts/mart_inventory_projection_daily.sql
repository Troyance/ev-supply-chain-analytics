{{ config(materialized='view', tags=['simulation']) }}
-- next 13 weeks of stock per part from today's position, with readable labels
select
    concat(f.site_id, ' ', p.part_number, ' ', p.part_name)   as part_label,
    f.site_id,
    f.part_id,
    f.projection_date,
    f.on_hand_units,
    f.reorder_point_units,
    f.safety_stock_units,
    f.ordered_units
from {{ ref('fct_inventory_projection') }} f
join {{ ref('dim_part') }} p using (part_key)
