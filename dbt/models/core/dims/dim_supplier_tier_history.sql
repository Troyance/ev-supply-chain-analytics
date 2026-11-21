-- scd type 2. collapse consecutive months with the same tier into one period (gaps and islands),
-- then each period is valid until the day before the next one starts.
with monthly as (
    select * from {{ ref('int_supplier_monthly_performance') }}
),

flagged as (
    select
        *,
        case when tier = lag(tier) over (partition by supplier_id order by month_start) then 0 else 1 end as is_new_period
    from monthly
),

numbered as (
    select
        *,
        sum(is_new_period) over (partition by supplier_id order by month_start rows unbounded preceding) as period_number
    from flagged
),

periods as (
    select
        supplier_id,
        period_number,
        tier,
        min(month_start)                                        as valid_from,
        sum(on_time_lines) / sum(delivery_lines)                as period_on_time_rate,
        sum(delivery_lines)                                     as period_delivery_lines
    from numbered
    group by all
)

select
    {{ dbt_utils.generate_surrogate_key(['supplier_id', 'valid_from']) }}                as supplier_tier_key,
    {{ dbt_utils.generate_surrogate_key(['supplier_id']) }}                              as supplier_key,
    supplier_id,
    tier,
    valid_from,
    coalesce(date_sub(lead(valid_from) over w, 1), date'9999-12-31')                    as valid_to,
    lead(valid_from) over w is null                                                     as is_current,
    cast(period_on_time_rate as double)                                                 as period_on_time_rate,
    cast(period_delivery_lines as bigint)                                               as period_delivery_lines
from periods
window w as (partition by supplier_id order by valid_from)
