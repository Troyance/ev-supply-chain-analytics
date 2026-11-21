select
    cast(part_id as bigint)                   as part_id,
    buffer_class,
    cast(planning_lead_time_days as int)      as planning_lead_time_days,
    cast(lead_time_stddev_days as double)     as lead_time_stddev_days,
    cast(review_period_days as int)           as review_period_days,
    cast(service_level_z as double)           as service_level_z,
    cast(pack_size as bigint)                 as pack_size,
    cast(moq as bigint)                       as moq
from {{ source('erp', 'erp_part_planning_parameters') }}
