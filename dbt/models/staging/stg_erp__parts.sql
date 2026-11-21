select
    cast(part_id as bigint)                as part_id,
    part_number,
    part_name,
    component_category,
    cast(standard_cost as decimal(18, 4))  as standard_cost
from {{ source('erp', 'dim_part_catalog') }}
