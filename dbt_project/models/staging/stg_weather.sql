-- Staging model for weather data
-- Lit la table raw_weather depuis PostgreSQL Warehouse (ou fixture pour CI)
with source as (
    {% if target.name == 'ci' %}
    select * from {{ ref('raw_weather_fixture') }}
    {% else %}
    select * from {{ source('warehouse_raw', 'raw_weather') }}
    {% endif %}
),

cleaned as (
    select
        airport_icao,
        airport_iata,
        cast(timestamp_utc as timestamp) as observation_ts,
        cast(date as date) as observation_date,
        cast(hour as integer) as observation_hour,
        cast(temperature_2m as double precision) as temp_celsius,
        cast(apparent_temperature as double precision) as apparent_temp_celsius,
        cast(precipitation as double precision) as precipitation_mm,
        cast(rain as double precision) as rain_mm,
        cast(wind_speed_10m as double precision) as wind_speed_kmh,
        cast(wind_gusts_10m as double precision) as wind_gusts_kmh,
        cast(wind_direction_10m as double precision) as wind_direction_deg,
        cast(visibility as double precision) as visibility_m,
        round(cast(visibility / 1000.0 as numeric), 2) as visibility_km,
        cast(cloud_cover as double precision) as cloud_cover_pct,
        cast(relative_humidity_2m as double precision) as humidity_pct,
        cast(pressure_msl as double precision) as pressure_hpa
    from source
),

deduped as (
    select
        *,
        row_number() over (
            partition by airport_icao, observation_ts
            order by observation_ts desc
        ) as rn
    from cleaned
)

select
    airport_icao,
    airport_iata,
    observation_ts,
    observation_date,
    observation_hour,
    temp_celsius,
    apparent_temp_celsius,
    precipitation_mm,
    rain_mm,
    wind_speed_kmh,
    wind_gusts_kmh,
    wind_direction_deg,
    visibility_m,
    visibility_km,
    cloud_cover_pct,
    humidity_pct,
    pressure_hpa
from deduped
where rn = 1
