-- one row per supplier. reliability, spend, and whether they're getting worse.
-- "recent" is the last 12 months of orders, "prior" the 12 before. the declining flag uses the slope of days late
-- over all 24 months, which is far less noisy than comparing two yearly percentages.
with deliveries as (
    select * from {{ ref('fct_po_deliveries') }}
),

as_of as (
    select max(order_date) as as_of_date from deliveries
),

lines as (
    select
        d.*,
        case when d.order_date > add_months(a.as_of_date, -12) then 'recent'
             when d.order_date > add_months(a.as_of_date, -24) then 'prior' end   as period,
        months_between(d.order_date, add_months(a.as_of_date, -24))               as month_idx
    from deliveries d
    cross join as_of a
),

stats as (
    select
        supplier_key,
        count(*)                                                                    as delivery_lines,
        sum(case when is_on_time then 1 else 0 end)                                 as on_time_lines,
        sum(net_amount)                                                             as spend,
        avg(lead_time_days)                                                         as avg_lead_time_days,
        stddev(lead_time_days)                                                      as lead_time_stddev_days,
        percentile(lead_time_days, 0.9)                                             as p90_lead_time_days,
        avg(case when days_late > 0 then days_late end)                             as avg_days_late_when_late,
        sum(case when period = 'recent' then 1 else 0 end)                          as recent_lines,
        sum(case when period = 'recent' and is_on_time then 1 else 0 end)           as recent_on_time_lines,
        sum(case when period = 'prior' then 1 else 0 end)                           as prior_lines,
        sum(case when period = 'prior' and is_on_time then 1 else 0 end)            as prior_on_time_lines,
        regr_slope(case when period is not null then days_late end,
                   case when period is not null then month_idx end)                 as lateness_trend
    from lines
    group by supplier_key
)

select
    s.supplier_key,
    d.supplier_id,
    d.supplier_name,
    d.country,
    d.region,
    d.current_tier                                                          as tier,
    s.delivery_lines,
    s.on_time_lines,
    cast(s.on_time_lines / s.delivery_lines as double)                      as on_time_rate,
    cast(s.recent_on_time_lines / nullif(s.recent_lines, 0) as double)      as on_time_rate_last_12m,
    cast(s.prior_on_time_lines / nullif(s.prior_lines, 0) as double)        as on_time_rate_prior_12m,
    cast(s.spend as decimal(18, 2))                                         as spend,
    cast(s.avg_lead_time_days as double)                                    as avg_lead_time_days,
    cast(s.lead_time_stddev_days as double)                                 as lead_time_stddev_days,
    cast(s.p90_lead_time_days as double)                                    as p90_lead_time_days,
    cast(s.avg_days_late_when_late as double)                               as avg_days_late_when_late,
    cast(s.lateness_trend as double)                                        as lateness_trend_days_per_month,
    coalesce(s.lateness_trend >= 0.15, false)                               as is_declining
from stats s
join {{ ref('dim_supplier') }} d using (supplier_key)
