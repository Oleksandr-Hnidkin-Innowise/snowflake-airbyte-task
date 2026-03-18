with source as (
    select * from {{ source('pagila', 'address') }}
),

renamed as (
    select
        cast(address_id as integer) as address_id,
        cast(address as text) as address_line_1,
        cast(address2 as text) as address_line_2,
        cast(district as text) as district,
        cast(city_id as integer) as city_id,
        cast(postal_code as text) as postal_code,
        cast(phone as text) as phone_number,
        cast(last_update as timestamp_tz) as updated_at
    from source
)

select * from renamed