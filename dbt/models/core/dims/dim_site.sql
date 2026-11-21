select
    {{ dbt_utils.generate_surrogate_key(['site_id']) }} as site_key,
    site_id,
    site_name,
    site_type
from {{ ref('stg_erp__sites') }}
