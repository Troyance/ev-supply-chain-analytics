{{ config(databricks_tags={"layer": "gold", "domain": "product_development", "certified": "true", "model_type": "mart"}) }}
-- weekly burndown per program and severity: found that week, closed that week, still open at week end.
-- built from the issue fact by checking each week against found / closed dates (a periodic snapshot from
-- an accumulating one)
with weeks as (
    select distinct week_start, date_add(week_start, 6) as week_end
    from {{ ref('dim_date') }}
    where date_day between date'2020-03-02' and current_date()
),

issues as (
    select i.*, p.program_name
    from {{ ref('fct_issue') }} i
    join {{ ref('dim_program') }} p using (program_key)
)

select
    w.week_start,
    i.program_key,
    i.program_name,
    i.severity,
    sum(case when i.found_date between w.week_start and w.week_end then 1 else 0 end)                           as issues_found,
    sum(case when i.closed_date between w.week_start and w.week_end then 1 else 0 end)                          as issues_closed,
    sum(case when i.found_date <= w.week_end and (i.closed_date is null or i.closed_date > w.week_end) then 1 else 0 end) as issues_open
from weeks w
join issues i on i.found_date <= w.week_end
group by all
having sum(case when i.found_date <= w.week_end and (i.closed_date is null or i.closed_date > w.week_end) then 1 else 0 end) > 0
    or sum(case when i.found_date between w.week_start and w.week_end then 1 else 0 end) > 0
