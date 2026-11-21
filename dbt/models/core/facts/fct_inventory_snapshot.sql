-- grain: snapshot date x site x part
select
    {{ date_key('i.snapshot_date') }}                              as snapshot_date_key,
    {{ dbt_utils.generate_surrogate_key(['i.site_id']) }}          as site_key,
    {{ dbt_utils.generate_surrogate_key(['i.part_id']) }}          as part_key,
    i.snapshot_date,
    i.site_id,
    i.part_id,
    i.on_hand_qty,
    i.on_order_qty,
    cast(i.on_hand_qty * p.standard_cost as decimal(18, 2))       as on_hand_value
from {{ ref('stg_wms__inventory_snapshot') }} i
join {{ ref('stg_erp__parts') }} p using (part_id)
