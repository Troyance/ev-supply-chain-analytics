-- the one part dimension: current parts with their mrp planning parameters, plus parts that engineering changes
-- superseded. retired parts stay in the dimension (real erps keep them too) so their purchase and demand history
-- still has a part to point at.
with current_parts as (
    select
        p.part_id, p.part_number, p.part_name, p.component_category, p.standard_cost,
        pp.buffer_class, pp.planning_lead_time_days, pp.lead_time_stddev_days, pp.review_period_days,
        pp.service_level_z, pp.pack_size, pp.moq,
        'Active'                    as lifecycle_status,
        cast(null as bigint)        as superseded_by_part_id
    from {{ ref('stg_erp__parts') }} p
    left join {{ ref('stg_erp__part_planning_parameters') }} pp using (part_id)
),

superseded_parts as (
    select
        i.item_id, i.item_number, i.item_name, i.component_category, s.standard_cost,
        cast(null as string), cast(null as int), cast(null as double), cast(null as int),
        cast(null as double), cast(null as bigint), cast(null as bigint),
        'Superseded', i.superseded_by_item_id
    from {{ ref('stg_plm__items') }} i
    join {{ ref('stg_erp__parts') }} s on s.part_id = i.superseded_by_item_id
    where i.lifecycle_status = 'Obsolete'
)

select {{ dbt_utils.generate_surrogate_key(['part_id']) }} as part_key, *
from (select * from current_parts union all select * from superseded_parts)
