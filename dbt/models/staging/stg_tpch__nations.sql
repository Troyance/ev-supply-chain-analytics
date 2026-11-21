select
    cast(n_nationkey as bigint) as nation_id,
    initcap(n_name)             as country,
    cast(n_regionkey as bigint) as region_id
from {{ source('tpch', 'bronze_nation') }}
