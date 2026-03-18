select
    f.film_id,
    f.title,
    f.description,
    f.release_year,
    f.rating,
    c.category_name
from {{ ref('stg_film') }} f
join {{ source('pagila', 'film_category') }} fc on f.film_id = fc.film_id
join {{ ref('stg_category') }} c on fc.category_id = c.category_id