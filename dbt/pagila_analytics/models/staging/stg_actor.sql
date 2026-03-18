with source as (
    select * from {{ source('pagila', 'actor') }}
),

renamed as (
    select
        cast(actor_id as integer) as actor_id,
        cast(first_name as text) as first_name,
        cast(last_name as text) as last_name,
        cast(last_update as timestamp_tz) as updated_at
    from source
)

select * from renamed