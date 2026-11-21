-- plant demand is dependent: what got built x how many of the part each vehicle takes.
-- service dc demand is independent and comes straight from the service system.
select
    b.week_start,
    b.site_id,
    bom.part_id,
    'dependent'                                    as demand_type,
    sum(b.units_built * bom.qty_per_vehicle)       as units
from {{ ref('stg_erp__vehicle_builds') }} b
join {{ ref('stg_erp__bom') }} bom using (vehicle_model_id)
group by all

union all

select week_start, site_id, part_id, 'independent', units
from {{ ref('stg_service__demand') }}
