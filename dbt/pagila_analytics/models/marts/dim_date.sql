with date_spine as (
    select 
        dateadd(day, seq4(), '2020-01-01') as date_day
    from table(generator(rowcount => 3650)) -- на 10 лет
)
select
    date_day as date_key,
    day(date_day) as day_of_month,
    month(date_day) as month_number,
    monthname(date_day) as month_name,
    year(date_day) as year,
    dayofweek(date_day) as day_of_week
from date_spine