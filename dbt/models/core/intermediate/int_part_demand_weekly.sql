-- plant demand is dependent: what got built x the bom that was valid that week. before an engineering change cut
-- in a new part, the plant consumed the old one, so history follows the effective-dated engineering bom.
-- service dc demand is independent and comes straight from the service system.
select
    b.week_start,
    b.site_id,
    l.part_id,
    'dependent'                         as demand_type,
    sum(b.units_built * l.qty)          as units
from {{ ref('stg_erp__vehicle_builds') }} b
join {{ ref('int_bom_leaf_lines') }} l
    on l.vehicle_model_id = b.vehicle_model_id
    and b.week_start between l.valid_from and l.valid_to
group by all

union all

select week_start, site_id, part_id, 'independent', units
from {{ ref('stg_service__demand') }}
