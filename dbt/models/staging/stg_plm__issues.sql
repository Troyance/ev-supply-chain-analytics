select issue_id, program_id, build_id, found_phase, cast(part_id as bigint) as part_id, component_category,
       severity, root_cause, found_date, closed_date
from {{ source('plm', 'plm_issues') }}
