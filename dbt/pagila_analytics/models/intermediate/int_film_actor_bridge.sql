select
    fa.film_id,
    fa.actor_id,
    a.first_name,
    a.last_name,
    concat(a.first_name, ' ', a.last_name) as actor_full_name
from {{ ref('stg_actor') }} a
join {{ source('pagila', 'film_actor') }} fa on a.actor_id = fa.actor_id