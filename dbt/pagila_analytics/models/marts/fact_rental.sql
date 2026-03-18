select
    rf.rental_id,
    rf.rental_at,
    rf.returned_at,
    rf.customer_id,
    rf.inventory_id,
    rf.payment_amount,
    i.film_id,
    i.store_id
from {{ ref('int_rental_facts') }} rf
join {{ ref('stg_inventory') }} i on rf.inventory_id = i.inventory_id