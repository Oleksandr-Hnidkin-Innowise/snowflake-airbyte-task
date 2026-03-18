with source as (
    select * from {{ source('pagila', 'inventory') }}
),

renamed as (
    select
        cast(inventory_id as integer) as inventory_id,
        cast(film_id as integer) as film_id,
        cast(store_id as integer) as store_id,
        cast(last_update as timestamp_tz) as updated_at
    from source
)

select * from renamed