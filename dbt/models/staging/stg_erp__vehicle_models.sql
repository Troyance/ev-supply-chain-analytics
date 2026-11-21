select model_id as vehicle_model_id, model_name as vehicle_model_name
from {{ source('erp', 'dim_vehicle_model') }}
