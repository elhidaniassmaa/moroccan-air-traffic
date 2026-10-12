-- Staging model for airports seed
with source as (
    select * from {{ ref('airports') }}
),

renamed as (
    select
        airport_icao,
        airport_iata,
        airport_name,
        city,
        country,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        cast(elevation_m as integer) as elevation_m,
        round(cast(elevation_m as double) * 3.28084, 1) as elevation_ft,
        cast(influence_radius_km as integer) as influence_radius_km
    from source
)

select * from renamed
