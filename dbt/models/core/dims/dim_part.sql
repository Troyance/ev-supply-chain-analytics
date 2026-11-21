-- the one part dimension: master data plus the mrp planning parameters
select
    {{ dbt_utils.generate_surrogate_key(['p.part_id']) }} as part_key,
    p.part_id,
    p.part_number,
    p.part_name,
    p.component_category,
    p.standard_cost,
    pp.buffer_class,
    pp.planning_lead_time_days,
    pp.lead_time_stddev_days,
    pp.review_period_days,
    pp.service_level_z,
    pp.pack_size,
    pp.moq
from {{ ref('stg_erp__parts') }} p
left join {{ ref('stg_erp__part_planning_parameters') }} pp using (part_id)
