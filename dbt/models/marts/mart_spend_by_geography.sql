-- one row per supplier country. sums only, so any ratio can be rebuilt at any level.
select
    s.region,
    s.country,
    count(distinct d.supplier_key)                                     as suppliers,
    count(*)                                                           as delivery_lines,
    sum(case when d.is_on_time then 1 else 0 end)                      as on_time_lines,
    cast(sum(d.net_amount) as decimal(18, 2))                          as spend,
    sum(d.lead_time_days)                                              as lead_time_days_total
from {{ ref('fct_po_deliveries') }} d
join {{ ref('dim_supplier') }} s using (supplier_key)
group by all
