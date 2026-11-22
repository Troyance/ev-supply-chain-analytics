-- one row per prototype build. phase_order lets reports sort EP < VP < PPV
select
    {{ dbt_utils.generate_surrogate_key(['build_id']) }}           as build_key,
    {{ dbt_utils.generate_surrogate_key(['program_id']) }}         as program_key,
    build_id,
    program_id,
    build_phase,
    case build_phase when 'EP' then 1 when 'VP' then 2 when 'PPV' then 3 end   as phase_order,
    build_number,
    planned_start_date,
    actual_start_date,
    datediff(actual_start_date, planned_start_date)                as slip_days,
    vehicles_built
from {{ ref('stg_plm__prototype_builds') }}
