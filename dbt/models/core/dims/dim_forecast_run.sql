select
    {{ dbt_utils.generate_surrogate_key(['run_id']) }} as forecast_run_key,
    run_id,
    run_type,
    forecast_type,
    model_name,
    model_version,
    mlflow_run_id,
    history_end_week,
    origin_week_start,
    origin_week_end,
    horizon_weeks,
    backtest_wape,
    backtest_bias,
    created_at,
    row_number() over (partition by run_type, forecast_type, model_name order by created_at desc) = 1 as is_latest
from {{ ref('stg_ml__forecast_runs') }}
