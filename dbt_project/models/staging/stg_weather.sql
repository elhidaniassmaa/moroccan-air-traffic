-- Staging model for weather data
-- Uses raw_weather parquet files or mock fallback for CI/demo environments
with source as (
    {% if target.name == 'ci' %}
    select * from {{ ref('raw_weather_fixture') }}
    {% else %}
    select * from read_parquet('{{ env_var("WEATHER_PARQUET_GLOB", "../data/lake/raw/weather/**/*.parquet") }}')
    {% endif %}
),

cleaned as (
    select
        airport_icao,
        airport_iata,
        cast(timestamp_utc as timestamp) as observation_ts,
        cast(date as date) as observation_date,
        cast(hour as integer) as observation_hour,
        cast(temperature_2m as double) as temp_celsius,
        cast(apparent_temperature as double) as apparent_temp_celsius,
        cast(precipitation as double) as precipitation_mm,
        cast(rain as double) as rain_mm,
        cast(wind_speed_10m as double) as wind_speed_kmh,
        cast(wind_gusts_10m as double) as wind_gusts_kmh,
        cast(wind_direction_10m as double) as wind_direction_deg,
        cast(visibility as double) as visibility_m,
        round(cast(visibility as double) / 1000.0, 2) as visibility_km,
        cast(cloud_cover as double) as cloud_cover_pct,
        cast(relative_humidity_2m as double) as humidity_pct,
        cast(pressure_msl as double) as pressure_hpa
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
