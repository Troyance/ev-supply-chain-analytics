select
    week_start,
    site_id,
    cast(part_id as bigint)  as part_id,
    cast(units as bigint)    as units
from {{ source('service', 'service_demand') }}
