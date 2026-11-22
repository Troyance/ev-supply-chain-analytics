select program_id, model_id as vehicle_model_id, site_id, program_name, ep_start as ep_start_date, sop_date
from {{ source('plm', 'plm_programs') }}
