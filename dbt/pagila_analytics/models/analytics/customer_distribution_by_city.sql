select 
    city_name, 
    count(*) as customer_count
from {{ ref('dim_customer') }}
group by 1
order by 2 desc