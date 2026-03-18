select
    a.full_name as actor_name,
    count(*) as children_film_count
from {{ ref('dim_actor') }} a
join {{ ref('int_film_actor_bridge') }} fab on a.actor_id = fab.actor_id
join {{ ref('dim_film') }} f on fab.film_id = f.film_id
where f.rating = 'G' 
   or f.category_name = 'Children'
group by 1
order by 2 desc
limit 10