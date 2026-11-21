select
    cast(part_id as bigint)                 as part_id,
    cast(supplier_id as bigint)             as supplier_id,
    sourcing_role,
    cast(volume_share as decimal(5, 4))     as volume_share,
    cast(unit_price as decimal(18, 4))      as unit_price
from {{ source('erp', 'dim_sourcing') }}
