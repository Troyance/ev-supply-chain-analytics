select
    cast(r_regionkey as bigint) as region_id,
    initcap(r_name)             as region
from {{ source('tpch', 'tpch_region') }}
