select 
    f.category_name, 
    sum(rev.amount) as total_revenue
from {{ ref('fact_revenue') }} rev
join {{ ref('dim_film') }} f on rev.rental_id in (select rental_id from {{ ref('fact_rental') }} where film_id = f.film_id)
group by 1
order by 2 desc
limit 1