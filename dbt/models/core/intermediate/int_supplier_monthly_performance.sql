-- one row per supplier per month they delivered something. the tier for a month is judged on the 12 months
-- *before* it, so a month's tier never uses that month's own deliveries.
with monthly as (
    select
        supplier_id,
        cast(date_trunc('MONTH', received_date) as date)                  as month_start,
        year(received_date) * 12 + month(received_date)                  as month_idx,
        count(*)                                                          as delivery_lines,
        sum(case when received_date <= promised_date then 1 else 0 end)  as on_time_lines
    from {{ ref('stg_erp__po_deliveries') }}
    group by all
),

rolled as (
    select
        *,
        sum(delivery_lines) over w as prior_12m_lines,
        sum(on_time_lines)  over w as prior_12m_on_time_lines
    from monthly
    window w as (partition by supplier_id order by month_idx range between 12 preceding and 1 preceding)
)

select
    supplier_id,
    month_start,
    delivery_lines,
    on_time_lines,
    prior_12m_lines,
    prior_12m_on_time_lines,
    prior_12m_on_time_lines / prior_12m_lines as rolling_on_time_rate,
    case
        when coalesce(prior_12m_lines, 0) < 20 then 'Unrated'     -- not enough history to judge
        when prior_12m_on_time_lines / prior_12m_lines >= 0.97 then 'A'
        when prior_12m_on_time_lines / prior_12m_lines >= 0.90 then 'B'
        else 'C'
    end as tier
from rolled
