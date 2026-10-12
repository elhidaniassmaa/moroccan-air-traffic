-- Intermediate model: evaluate flight phases and classify flight paths
with flights as (
    select * from {{ ref('stg_flights') }}
),

classified as (
    select
        icao24,
        callsign,
        origin_country,
        position_ts,
        event_date,
        latitude,
        longitude,
        baro_altitude_m,
        baro_altitude_ft,
        velocity_ms,
        velocity_kmh,
        vertical_rate_ms,
        heading_deg,
        on_ground,
        nearest_airport_icao,
        nearest_airport_iata,
        distance_to_airport_km,
        {{ flight_phase('on_ground', 'baro_altitude_m', 'velocity_ms', 'vertical_rate_ms') }} as computed_flight_phase
    from flights
)

select * from classified
