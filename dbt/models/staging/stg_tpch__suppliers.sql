select
    cast(s_suppkey as bigint)   as supplier_id,
    trim(s_name)                as supplier_name,
    cast(s_nationkey as bigint) as nation_id
from {{ source('tpch', 'bronze_supplier') }}
