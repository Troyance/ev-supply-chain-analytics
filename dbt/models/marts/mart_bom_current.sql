{{ config(databricks_tags={"layer": "gold", "domain": "product_development", "certified": "true", "model_type": "mart"}) }}
-- today's bom flattened: one row per vehicle model x part, with the system and assembly it sits under
with explosion as (
    select vehicle_model_id, item_id, extended_qty, split(path, ' > ') as ids
    from {{ ref('int_bom_explosion') }}
    where as_of_label = 'current' and bom_level = 3
)

select
    e.vehicle_model_id,
    sys.item_name                                                 as system_name,
    asm.item_name                                                 as assembly_name,
    part.part_key,
    part.item_number                                              as part_number,
    part.item_name                                                as part_name,
    part.component_category,
    cast(e.extended_qty as int)                                   as qty_per_vehicle,
    p.standard_cost,
    cast(e.extended_qty * p.standard_cost as decimal(18, 2))      as extended_cost
from explosion e
join {{ ref('dim_item') }} sys  on sys.item_id = cast(e.ids[1] as bigint)
join {{ ref('dim_item') }} asm  on asm.item_id = cast(e.ids[2] as bigint)
join {{ ref('dim_item') }} part on part.item_id = e.item_id
join {{ ref('dim_part') }} p    on p.part_key = part.part_key
