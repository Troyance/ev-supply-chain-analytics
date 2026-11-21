-- the generator also writes each supplier's true late probability, lateness and whether it is secretly
-- slipping. a real erp wouldn't have those, so they stop here and never reach analytics.
select
    cast(supplier_id as bigint)           as supplier_id,
    cast(quoted_lead_time_days as int)    as quoted_lead_time_days,
    cast(transit_days as int)             as transit_days
from {{ source('erp', 'dim_active_supplier') }}
