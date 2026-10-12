-- Intermediate model: aggregate weather on an hourly dimension with condition classifications
with weather as (
    select * from {{ ref('stg_weather') }}
),

classified as (
    select
        airport_icao,
        airport_iata,
        observation_date,
        observation_hour,
        observation_ts,
        temp_celsius,
        apparent_temp_celsius,
        precipitation_mm,
        wind_speed_kmh,
        wind_gusts_kmh,
        wind_direction_deg,
        visibility_km,
        cloud_cover_pct,
        humidity_pct,
        pressure_hpa,
        case
            when precipitation_mm > 5.0 or wind_gusts_kmh > 50.0 or visibility_km < 2.0 then 'severe'
            when precipitation_mm > 1.0 or wind_gusts_kmh > 35.0 or visibility_km < 5.0 then 'moderate'
            else 'favorable'
        end as weather_severity
    from weather
)

select * from classified
