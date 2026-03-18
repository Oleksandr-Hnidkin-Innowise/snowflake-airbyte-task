select
    customer_id,
    first_name,
    last_name,
    email,
    city_name,
    district as region,
    is_active
from {{ ref('int_customer_enriched') }}