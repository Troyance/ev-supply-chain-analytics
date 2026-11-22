-- one row per vehicle program (a model launching at a plant)
select
    {{ dbt_utils.generate_surrogate_key(['program_id']) }}         as program_key,
    {{ dbt_utils.generate_surrogate_key(['vehicle_model_id']) }}   as vehicle_model_key,
    {{ dbt_utils.generate_surrogate_key(['site_id']) }}            as site_key,
    program_id,
    program_name,
    vehicle_model_id,
    site_id,
    ep_start_date,
    sop_date
from {{ ref('stg_plm__programs') }}
