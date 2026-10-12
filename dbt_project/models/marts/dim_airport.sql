-- Dimension: Airports
with airports as (
    select * from {{ ref('stg_airports') }}
)

select
    airport_icao,
    airport_iata,
    airport_name,
    city,
    country,
    latitude,
    longitude,
    elevation_m,
    elevation_ft,
    influence_radius_km
from airports
