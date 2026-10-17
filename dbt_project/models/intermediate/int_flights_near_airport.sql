-- Intermediate model: flights within airport influence zone
with flights as (
    select * from {{ ref('stg_flights') }}
),

airports as (
    select * from {{ ref('stg_airports') }}
),

matched as (
    select
        f.icao24,
        f.callsign,
        f.origin_country,
        f.position_ts,
        f.last_contact_ts,
        f.latitude,
        f.longitude,
        f.baro_altitude_m,
        f.baro_altitude_ft,
        f.velocity_ms,
        f.velocity_kmh,
        f.vertical_rate_ms,
        f.heading_deg,
        f.on_ground,
        f.squawk,
        f.event_date,
        a.airport_icao,
        a.airport_iata,
        a.airport_name,
        a.city,
        round(cast({{ haversine_km('f.latitude', 'f.longitude', 'a.latitude', 'a.longitude') }} as numeric), 2) as distance_to_airport_km,
        a.influence_radius_km
    from flights f
    cross join airports a
)

select
    icao24,
    callsign,
    origin_country,
    position_ts,
    last_contact_ts,
    latitude,
    longitude,
    baro_altitude_m,
    baro_altitude_ft,
    velocity_ms,
    velocity_kmh,
    vertical_rate_ms,
    heading_deg,
    on_ground,
    squawk,
    event_date,
    airport_icao,
    airport_iata,
    airport_name,
    city,
    distance_to_airport_km
from matched
where distance_to_airport_km <= influence_radius_km
