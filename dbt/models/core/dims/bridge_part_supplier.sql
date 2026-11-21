-- parts and suppliers are many to many (dual sourcing), so the relationship lives in a bridge, not on dim_part
select
    {{ dbt_utils.generate_surrogate_key(['part_id']) }}      as part_key,
    {{ dbt_utils.generate_surrogate_key(['supplier_id']) }}  as supplier_key,
    part_id,
    supplier_id,
    sourcing_role,
    volume_share,
    unit_price
from {{ ref('stg_erp__sourcing') }}
