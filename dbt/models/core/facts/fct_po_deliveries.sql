-- grain: one delivery line. the supplier tier is looked up as of the receipt date (scd2),
-- so old deliveries are judged by the tier the supplier had back then.
with deliveries as (
    select * from {{ ref('stg_erp__po_deliveries') }}
)

select
    d.po_line_id,
    {{ date_key('d.order_date') }}                                        as order_date_key,
    {{ date_key('d.promised_date') }}                                     as promised_date_key,
    {{ date_key('d.received_date') }}                                     as received_date_key,
    {{ dbt_utils.generate_surrogate_key(['d.site_id']) }}                 as site_key,
    {{ dbt_utils.generate_surrogate_key(['d.part_id']) }}                 as part_key,
    {{ dbt_utils.generate_surrogate_key(['d.supplier_id']) }}             as supplier_key,
    t.supplier_tier_key,
    d.site_id,
    d.part_id,
    d.supplier_id,
    d.sourcing_role,
    d.ship_mode,
    d.order_date,
    d.ship_date,
    d.promised_date,
    d.received_date,
    d.quantity,
    d.net_amount,
    datediff(d.received_date, d.order_date)                              as lead_time_days,
    datediff(d.received_date, d.promised_date)                           as days_late,
    d.received_date <= d.promised_date                                   as is_on_time
from deliveries d
left join {{ ref('dim_supplier_tier_history') }} t
    on t.supplier_id = d.supplier_id
    and d.received_date between t.valid_from and t.valid_to
