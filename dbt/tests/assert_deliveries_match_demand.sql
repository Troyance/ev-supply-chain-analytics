-- over each part's whole history, units delivered should roughly match units demanded at the plants.
-- not exact: the last weeks' deliveries haven't arrived yet and lines are rounded. a big gap would mean
-- purchasing and demand disagree about which part was in the bom when.
{{ config(severity='warn') }}
with demand as (
    select part_id, sum(units) as demanded
    from {{ ref('fct_part_demand') }}
    where demand_type = 'dependent'
    group by part_id
),

delivered as (
    select part_id, sum(quantity) as delivered
    from {{ ref('fct_po_deliveries') }}
    group by part_id
)

select coalesce(d.part_id, v.part_id) as part_id, d.demanded, v.delivered
from demand d
full outer join delivered v using (part_id)
where coalesce(d.demanded, v.delivered) >= 100            -- tiny volumes round badly across daily delivery lines
  and (coalesce(v.delivered, 0) < 0.85 * coalesce(d.demanded, 0)
   or coalesce(v.delivered, 0) > 1.05 * coalesce(d.demanded, 0))
