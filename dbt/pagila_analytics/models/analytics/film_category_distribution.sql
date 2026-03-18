select 
    category_name, 
    count(*) as total_films
from {{ ref('dim_film') }}
group by 1
order by 2 desc