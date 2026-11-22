select eco_id, program_id, reason, priority, component_category, issue_id, status,
       created_date, submitted_date, reviewed_date, approved_date, implemented_date
from {{ source('plm', 'plm_engineering_changes') }}
