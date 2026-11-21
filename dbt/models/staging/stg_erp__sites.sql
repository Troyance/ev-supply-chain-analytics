select site_id, site_name, site_type
from {{ source('erp', 'dim_site') }}
