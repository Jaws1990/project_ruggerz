with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="dateadd(year, 1, current_date())"
    ) }}
)

select
    cast(date_day as DATE) as date_day,
    extract(year from date_day)      as year,
    extract(month from date_day)     as month,
    extract(day from date_day)       as day_of_month,
    extract(dayofweek from date_day) as day_of_week,
    date_format(date_day, 'EEEE')    as day_name,
    date_format(date_day, 'MMMM')    as month_name,
    extract(quarter from date_day)   as quarter,
    case when extract(dayofweek from date_day) in (1,7)
         then true else false end    as is_weekend
from spine