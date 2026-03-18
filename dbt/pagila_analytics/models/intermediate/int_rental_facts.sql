select
    r.rental_id,
    r.rental_at,
    r.returned_at,
    r.customer_id,
    r.inventory_id,
    p.amount as payment_amount,
    p.paid_at
from {{ ref('stg_rental') }} r
left join {{ ref('stg_payment') }} p on r.rental_id = p.rental_id