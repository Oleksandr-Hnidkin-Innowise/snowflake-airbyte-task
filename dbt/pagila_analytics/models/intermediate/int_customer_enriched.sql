select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.is_active,
    city.city_name,
    addr.district
from {{ ref('stg_customer') }} c
join {{ ref('stg_address') }} addr on c.address_id = addr.address_id
join {{ ref('stg_city') }} city on addr.city_id = city.city_id