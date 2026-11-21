select
    week_start,
    site_id,
    cast(part_id as bigint)  as part_id,
    cast(units as bigint)    as units
from {{ source('service', 'fct_service_demand') }}
