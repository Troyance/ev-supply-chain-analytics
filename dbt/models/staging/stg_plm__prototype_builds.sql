select build_id, program_id, build_phase, cast(build_number as int) as build_number, planned_start_date,
       actual_start_date, cast(vehicles_built as int) as vehicles_built
from {{ source('plm', 'plm_prototype_builds') }}
