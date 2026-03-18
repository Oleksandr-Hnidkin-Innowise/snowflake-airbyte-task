with source as (
    select * from {{ source('pagila', 'payment') }}
),

renamed as (
    select
        cast(payment_id as integer) as payment_id,
        cast(customer_id as integer) as customer_id,
        cast(staff_id as integer) as staff_id,
        cast(rental_id as integer) as rental_id,
        cast(amount as float) as amount,
        cast(payment_date as timestamp_tz) as paid_at
    from source
)

select * from renamed