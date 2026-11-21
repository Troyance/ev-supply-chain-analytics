-- grain: week x site x part
select
    {{ date_key('week_start') }}                                as week_date_key,
    {{ dbt_utils.generate_surrogate_key(['site_id']) }}         as site_key,
    {{ dbt_utils.generate_surrogate_key(['part_id']) }}         as part_key,
    week_start,
    site_id,
    part_id,
    demand_type,
    cast(units as bigint)                                       as units
from {{ ref('int_part_demand_weekly') }}
