-- accumulating snapshot: one row per eco, a date column per milestone, filled in as the change moves along.
-- durations between milestones are the workflow kpis. the phase is when the change was raised.
with ecos as (
    select * from {{ ref('stg_plm__engineering_changes') }}
),

phase_at_creation as (
    select e.eco_id, max_by(b.build_phase, b.actual_start_date) as last_build_phase
    from ecos e
    join {{ ref('stg_plm__prototype_builds') }} b
        on b.program_id = e.program_id and b.actual_start_date <= e.created_date
    group by e.eco_id
)

select
    {{ dbt_utils.generate_surrogate_key(['e.eco_id']) }}                           as eco_key,
    {{ dbt_utils.generate_surrogate_key(['e.program_id']) }}                       as program_key,
    case when e.issue_id is not null then {{ dbt_utils.generate_surrogate_key(['e.issue_id']) }} end as originating_issue_key,
    {{ date_key('e.created_date') }}                                               as created_date_key,
    {{ date_key('e.implemented_date') }}                                           as implemented_date_key,
    e.eco_id,
    e.program_id,
    e.reason,
    e.priority,
    e.component_category,
    e.status,
    case when e.created_date >= p.sop_date then 'Production'
         else coalesce(ph.last_build_phase, 'EP') end                             as raised_in_phase,
    e.created_date,
    e.submitted_date,
    e.reviewed_date,
    e.approved_date,
    e.implemented_date,
    datediff(e.submitted_date, e.created_date)                                    as days_in_draft,
    datediff(e.reviewed_date, e.submitted_date)                                   as days_in_review,
    datediff(e.approved_date, e.reviewed_date)                                    as days_to_approve,
    datediff(e.implemented_date, e.approved_date)                                 as days_to_implement,
    datediff(e.implemented_date, e.created_date)                                  as cycle_time_days,
    e.status not in ('Implemented', 'Rejected')                                   as is_open,
    datediff(current_date(), e.created_date)                                      as age_days
from ecos e
join {{ ref('stg_plm__programs') }} p using (program_id)
left join phase_at_creation ph using (eco_id)
