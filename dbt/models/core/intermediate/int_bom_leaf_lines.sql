-- assembly > part lines from the engineering bom, with the vehicle model they belong to and when they were valid.
-- upper levels (vehicle > system > assembly) never change, so the leaf lines carry all the history.
select
    split(a.item_number, '-')[2]          as vehicle_model_id,
    l.child_item_id                       as part_id,
    l.qty,
    l.effective_from                      as valid_from,
    coalesce(l.effective_to, date'9999-12-31') as valid_to
from {{ ref('stg_plm__bom_lines') }} l
join {{ ref('stg_plm__items') }} a on a.item_id = l.parent_item_id and a.item_type = 'assembly'
