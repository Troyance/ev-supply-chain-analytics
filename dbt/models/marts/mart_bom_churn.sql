{{ config(databricks_tags={"layer": "gold", "domain": "product_development", "certified": "true", "model_type": "mart"}) }}
-- how much of each program's bom changed in each phase. late churn (after PPV) is the expensive kind:
-- tooling is cut and suppliers are ramping
with windows as (
    select program_key, program_id, phase, phase_start,
           coalesce(lead(phase_start) over (partition by program_key order by phase_start), current_date()) as phase_end
    from (
        select program_key, program_id, build_phase as phase, min(actual_start_date) as phase_start
        from {{ ref('dim_prototype_build') }}
        group by all
        union all
        select program_key, program_id, 'Production', sop_date
        from {{ ref('dim_program') }}
    )
),

bom_size as (
    select vehicle_model_id, count(*) as bom_parts
    from {{ ref('bridge_vehicle_model_part') }}
    group by vehicle_model_id
),

changes as (
    select e.program_key, e.implemented_date, b.part_key
    from {{ ref('fct_engineering_change') }} e
    join {{ ref('bridge_eco_item') }} b using (eco_key)
    where e.status = 'Implemented'
)

select
    w.program_key,
    p.program_name,
    p.vehicle_model_id,
    w.phase,
    case w.phase when 'EP' then 1 when 'VP' then 2 when 'PPV' then 3 else 4 end   as phase_order,
    w.phase_start,
    w.phase_end,
    count(c.part_key)                                                            as parts_changed,
    s.bom_parts,
    cast(count(c.part_key) / s.bom_parts as double)                              as bom_churn_rate
from windows w
join {{ ref('dim_program') }} p using (program_key)
join bom_size s using (vehicle_model_id)
left join changes c
    on c.program_key = w.program_key
    and c.implemented_date >= w.phase_start and c.implemented_date < w.phase_end
group by all
