select
    {{ dbt_utils.generate_surrogate_key(['policy_id']) }} as policy_key,
    policy_id,
    policy_name,
    review_type,
    description
from {{ ref('policies') }}
