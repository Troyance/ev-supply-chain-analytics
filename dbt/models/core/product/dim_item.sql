-- the plm item master: vehicles, systems, assemblies, current parts and superseded parts.
-- released parts share part_key with dim_part, so engineering and purchasing join on the same key
select
    {{ dbt_utils.generate_surrogate_key(['item_id']) }}            as item_key,
    case when item_type = 'part' and lifecycle_status = 'Released'
         then {{ dbt_utils.generate_surrogate_key(['item_id']) }} end   as part_key,
    item_id,
    item_number,
    item_name,
    item_type,
    case item_type when 'vehicle' then 0 when 'system' then 1 when 'assembly' then 2 else 3 end as bom_level,
    component_category,
    lifecycle_status
from {{ ref('stg_plm__items') }}
