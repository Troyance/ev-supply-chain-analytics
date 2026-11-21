select
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
    wape   as backtest_wape,
    bias   as backtest_bias,
    created_at
from {{ source('ml', 'forecast_run') }}
