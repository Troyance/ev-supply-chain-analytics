select
    {{ dbt_utils.generate_surrogate_key(['vehicle_model_id']) }} as vehicle_model_key,
    vehicle_model_id,
    vehicle_model_name
from {{ ref('stg_erp__vehicle_models') }}
