-- one row per order month x component category x supplier region
select
    dt.month_start                                                     as order_month,
    p.component_category,
    s.region                                                           as supplier_region,
    count(*)                                                           as delivery_lines,
    sum(case when d.is_on_time then 1 else 0 end)                      as on_time_lines,
    cast(sum(d.net_amount) as decimal(18, 2))                          as spend,
    sum(d.lead_time_days)                                              as lead_time_days_total,
    max(dt.chip_shortage_severity)                                     as chip_shortage_severity
from {{ ref('fct_po_deliveries') }} d
join {{ ref('dim_date') }} dt on dt.date_key = d.order_date_key
join {{ ref('dim_part') }} p using (part_key)
join {{ ref('dim_supplier') }} s using (supplier_key)
group by all
