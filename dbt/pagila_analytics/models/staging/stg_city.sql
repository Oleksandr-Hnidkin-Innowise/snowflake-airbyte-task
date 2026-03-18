with source as (
    select * from {{ source('pagila', 'city') }}
),

renamed as (
    select
        cast(city_id as integer) as city_id,
        cast(city as text) as city_name,
        cast(country_id as integer) as country_id,
        cast(last_update as timestamp_tz) as updated_at
    from source
)

select * from renamed