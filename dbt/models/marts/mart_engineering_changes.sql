{{ config(databricks_tags={"layer": "gold", "domain": "product_development", "certified": "true", "model_type": "mart"}) }}
-- one row per engineering change, with names and the time spent in each stage
with affected as (
    select eco_key, count(*) as parts_affected
    from {{ ref('bridge_eco_item') }}
    group by eco_key
)

select
    e.eco_key,
    e.program_key,
    e.eco_id,
    p.program_name,
    p.vehicle_model_id,
    p.site_id,
    e.reason,
    e.priority,
    e.component_category,
    e.status,
    e.raised_in_phase,
    case e.raised_in_phase when 'EP' then 1 when 'VP' then 2 when 'PPV' then 3 else 4 end   as phase_order,
    e.originating_issue_key is not null                                                     as from_issue,
    coalesce(a.parts_affected, 0)                                                           as parts_affected,
    e.created_date,
    e.implemented_date,
    e.days_in_draft,
    e.days_in_review,
    e.days_to_approve,
    e.days_to_implement,
    e.cycle_time_days,
    e.is_open,
    e.age_days
from {{ ref('fct_engineering_change') }} e
join {{ ref('dim_program') }} p using (program_key)
left join affected a using (eco_key)
