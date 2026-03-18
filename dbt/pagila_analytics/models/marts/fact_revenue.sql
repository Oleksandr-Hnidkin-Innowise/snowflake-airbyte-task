select
    payment_id,
    customer_id,
    rental_id,
    amount,
    paid_at as payment_date
from {{ ref('stg_payment') }}