select 
    a.actor_full_name as full_name, 
    count(r.rental_id) as total_rentals
from {{ ref('fact_rental') }} r
join {{ ref('dim_film') }} f on r.film_id = f.film_id
join {{ ref('int_film_actor_bridge') }} a on f.film_id = a.film_id
group by 1
order by 2 desc
limit 10