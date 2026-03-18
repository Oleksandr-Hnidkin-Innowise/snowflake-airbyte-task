with source as (
    select * from {{ source('pagila', 'category') }}
),

renamed as (
    select
        cast(category_id as integer) as category_id,
        cast(name as text) as category_name,
        cast(last_update as timestamp_tz) as updated_at
    from source
)

select * from renamed