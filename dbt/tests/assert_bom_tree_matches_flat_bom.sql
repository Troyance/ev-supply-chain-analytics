-- engineering's multi-level bom, exploded as of today, must give exactly the flat bom purchasing uses.
-- returns every model x part where they disagree, so any row fails the test.
with tree as (
    select e.vehicle_model_id, e.item_id as part_id, sum(e.extended_qty) as qty
    from {{ ref('int_bom_explosion') }} e
    join {{ ref('dim_item') }} i using (item_id)
    where e.as_of_label = 'current' and i.item_type = 'part'
    group by all
),

flat as (
    select vehicle_model_id, part_id, qty_per_vehicle as qty
    from {{ ref('bridge_vehicle_model_part') }}
)

select coalesce(t.vehicle_model_id, f.vehicle_model_id) as vehicle_model_id,
       coalesce(t.part_id, f.part_id) as part_id, t.qty as tree_qty, f.qty as flat_qty
from tree t
full outer join flat f using (vehicle_model_id, part_id)
where t.qty is null or f.qty is null or t.qty != f.qty
