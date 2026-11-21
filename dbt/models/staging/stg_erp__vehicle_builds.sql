select
    week_start,
    site_id,
    model_id               as vehicle_model_id,
    cast(units as bigint)  as units_built
from {{ source('erp', 'fct_vehicle_builds') }}
