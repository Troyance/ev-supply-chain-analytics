with days as (
    {{ dbt_utils.date_spine(datepart="day", start_date="cast('2019-01-01' as date)", end_date="cast('2028-01-01' as date)") }}
)

select
    {{ date_key('date_day') }}                                        as date_key,
    cast(date_day as date)                                           as date_day,
    cast(date_trunc('WEEK', date_day) as date)                       as week_start,
    cast(weekofyear(date_day) as int)                                as iso_week,
    cast(year(date_add(date_trunc('WEEK', date_day), 3)) as int)     as iso_year,
    cast(date_trunc('MONTH', date_day) as date)                      as month_start,
    cast(month(date_day) as int)                                     as month_of_year,
    cast(quarter(date_day) as int)                                   as quarter_of_year,
    cast(year(date_day) as int)                                      as calendar_year,
    cast(((dayofweek(date_day) + 5) % 7) + 1 as int)                 as iso_day_of_week,
    dayofweek(date_day) in (1, 7)                                    as is_weekend,
    weekofyear(date_trunc('WEEK', date_day)) in (1, 27, 28, 52, 53)  as is_shutdown_week,
    month(date_day) in (3, 6, 9, 12) and day(date_trunc('WEEK', date_day)) >= 16 as is_quarter_end_push,
    cast({{ shortage_severity('cast(date_day as date)') }} as double) as chip_shortage_severity
from days
