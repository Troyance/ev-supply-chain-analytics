-- vehicle recall lines (record type V), flagged as battery-electric from the curated seed and mapped to a
-- component category. a line can match several component patterns, the lowest priority number wins.
with lines as (
    select * from {{ ref('stg_nhtsa__recalls') }}
    where recall_type = 'V'
),

ev as (
    select l.record_id, any_value(e.powertrain) as powertrain
    from lines l
    join {{ ref('ev_models') }} e
        on l.make = e.make and l.model like e.model_like and coalesce(l.model_year, 0) >= e.min_model_year
    group by l.record_id
),

component as (
    select l.record_id, min_by(m.component_category, m.priority) as component_category
    from lines l
    join {{ ref('recall_component_map') }} m on l.component_name like m.component_like
    group by l.record_id
)

select l.*, ev.powertrain, ev.powertrain is not null as is_ev, c.component_category
from lines l
left join ev using (record_id)
left join component c using (record_id)
