with source as (
    select * from {{ source('pagila', 'film') }}
),

renamed as (
    select
        cast(film_id as integer) as film_id,
        cast(title as text) as title,
        cast(description as text) as description,
        cast(release_year as integer) as release_year,
        cast(language_id as integer) as language_id,
        cast(original_language_id as integer) as original_language_id,
        cast(rental_duration as integer) as rental_duration,
        cast(rental_rate as float) as rental_rate,
        cast(length as integer) as length,
        cast(replacement_cost as float) as replacement_cost,
        cast(rating as text) as rating,
        special_features, -- VARIANT 
        cast(last_update as timestamp_tz) as updated_at
    from source
)

select * from renamed