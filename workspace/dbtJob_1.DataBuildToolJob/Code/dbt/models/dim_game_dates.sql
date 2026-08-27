select
    date_key as game_date,
    year,
    month,
    day_of_month,
    day_of_week,
    day_name,
    month_name,
    quarter,
    is_weekend
from {{ ref('dim_dates') }}