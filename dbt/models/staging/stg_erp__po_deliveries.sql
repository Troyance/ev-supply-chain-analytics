-- lead time, lateness and on-time are recomputed downstream from the dates rather than trusted from the source
select
    cast(po_line_id as bigint)            as po_line_id,
    site_id,
    cast(part_id as bigint)               as part_id,
    cast(supplier_id as bigint)           as supplier_id,
    sourcing_role,
    cast(quantity as bigint)              as quantity,
    cast(net_amount as decimal(18, 2))    as net_amount,
    order_date,
    ship_date,
    promised_date,
    received_date,
    ship_mode
from {{ source('erp', 'erp_po_deliveries') }}
