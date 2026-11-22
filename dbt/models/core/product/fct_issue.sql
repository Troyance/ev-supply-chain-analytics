-- one row per issue (found -> closed). links to the build that found it and the eco it raised, if any
with eco_from_issue as (
    select issue_id, min(eco_id) as eco_id
    from {{ ref('stg_plm__engineering_changes') }}
    where issue_id is not null
    group by issue_id
)

select
    {{ dbt_utils.generate_surrogate_key(['i.issue_id']) }}                         as issue_key,
    {{ dbt_utils.generate_surrogate_key(['i.program_id']) }}                       as program_key,
    case when i.build_id is not null then {{ dbt_utils.generate_surrogate_key(['i.build_id']) }} end as build_key,
    {{ dbt_utils.generate_surrogate_key(['i.part_id']) }}                          as part_key,
    case when e.eco_id is not null then {{ dbt_utils.generate_surrogate_key(['e.eco_id']) }} end as eco_key,
    {{ date_key('i.found_date') }}                                                 as found_date_key,
    i.issue_id,
    i.program_id,
    i.build_id,
    i.found_phase,
    i.part_id,
    i.component_category,
    i.severity,
    i.root_cause,
    i.found_date,
    i.closed_date,
    datediff(i.closed_date, i.found_date)                                         as days_to_close,
    i.closed_date is null                                                         as is_open,
    e.eco_id is not null                                                          as raised_eco
from {{ ref('stg_plm__issues') }} i
left join eco_from_issue e using (issue_id)
