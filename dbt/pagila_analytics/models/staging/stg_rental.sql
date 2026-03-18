with source as (
    select * from {{ source('pagila', 'rental') }}
),

renamed as (
    select
        cast(rental_id as integer) as rental_id,
        cast(rental_date as timestamp_tz) as rental_at,
        cast(inventory_id as integer) as inventory_id,
        cast(customer_id as integer) as customer_id,
        cast(return_date as timestamp_tz) as returned_at,
        cast(staff_id as integer) as staff_id,
        cast(last_update as timestamp_tz) as updated_at
    from source
)

select * from renamed