select
    run_id,
    forecast_type,
    model_name,
    origin_week,
    target_week,
    horizon_weeks,
    site_id,
    model_id                    as vehicle_model_id,
    cast(part_id as bigint)     as part_id,
    forecast_p10,
    forecast_p50,
    forecast_p90
from {{ source('ml', 'fct_forecast') }}
