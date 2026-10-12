-- Staging model for silver flights
-- Reads either fixture (for CI) or parquet lakehouse files
with source as (
    {% if target.name == 'ci' %}
    select * from {{ ref('silver_flights_fixture') }}
    {% else %}
    select * from read_parquet('{{ env_var("FLIGHTS_PARQUET_GLOB", "../data/lake/silver/flights/**/*.parquet") }}')
    {% endif %}
),

cleaned as (
    select
        cast(icao24 as varchar) as icao24,
        upper(trim(cast(callsign as varchar))) as callsign,
        cast(origin_country as varchar) as origin_country,
        to_timestamp(cast(time_position as bigint)) as position_ts,
        to_timestamp(cast(last_contact as bigint)) as last_contact_ts,
        cast(longitude as double) as longitude,
        cast(latitude as double) as latitude,
        cast(baro_altitude as double) as baro_altitude_m,
        round(cast(baro_altitude as double) * 3.28084, 1) as baro_altitude_ft,
        cast(on_ground as boolean) as on_ground,
        cast(velocity as double) as velocity_ms,
        round(cast(velocity as double) * 3.6, 2) as velocity_kmh,
        cast(true_track as double) as heading_deg,
        cast(vertical_rate as double) as vertical_rate_ms,
        cast(geo_altitude as double) as geo_altitude_m,
        cast(squawk as varchar) as squawk,
        coalesce(cast(spi as boolean), false) as spi,
        cast(position_source as integer) as position_source,
        cast(category as integer) as category,
        cast(nearest_airport_icao as varchar) as nearest_airport_icao,
        cast(nearest_airport_iata as varchar) as nearest_airport_iata,
        cast(distance_to_airport_km as double) as distance_to_airport_km,
        cast(flight_phase as varchar) as flight_phase,
        cast(event_date as date) as event_date,
        cast(ingestion_ts as timestamp) as ingestion_ts
    from source
    where latitude between 21.0 and 36.0
      and longitude between -17.0 and -1.0
),

deduped as (
    select
        *,
        row_number() over (
            partition by icao24, position_ts
            order by ingestion_ts desc
        ) as rn
    from cleaned
)

select
    icao24,
    callsign,
    origin_country,
    position_ts,
    last_contact_ts,
    longitude,
    latitude,
    baro_altitude_m,
    baro_altitude_ft,
    on_ground,
    velocity_ms,
    velocity_kmh,
    heading_deg,
    vertical_rate_ms,
    geo_altitude_m,
    squawk,
    spi,
    position_source,
    category,
    nearest_airport_icao,
    nearest_airport_iata,
    distance_to_airport_km,
    flight_phase,
    event_date,
    ingestion_ts
from deduped
where rn = 1
