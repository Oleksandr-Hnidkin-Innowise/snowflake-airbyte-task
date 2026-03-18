select
    f.category_name,
    sum(datediff('hour', r.rental_at, r.returned_at)) as total_rental_hours,
    avg(datediff('hour', r.rental_at, r.returned_at)) as avg_rental_hours
from {{ ref('fact_rental') }} r
join {{ ref('dim_film') }} f on r.film_id = f.film_id
where r.returned_at is not null
group by 1
order by 2 desc