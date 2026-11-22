-- grain: one nhtsa record - a campaign x make x model x model year x component. no units here on purpose
select
    {{ dbt_utils.generate_surrogate_key(['record_id']) }}            as recall_line_key,
    {{ dbt_utils.generate_surrogate_key(['campaign_id']) }}          as recall_campaign_key,
    record_id,
    campaign_id,
    make,
    model,
    model_year,
    component_name,
    component_category,
    powertrain,
    is_ev
from {{ ref('int_recall_vehicle_lines') }}
