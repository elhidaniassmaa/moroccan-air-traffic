-- Mart: correlation between airport flight movements and prevailing weather
with movements as (
    select * from {{ ref('fct_airport_movements_hourly') }}
),

weather as (
    select * from {{ ref('int_weather_hourly') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

joined as (
    select
        m.movement_hour_id,
        m.airport_icao,
        m.airport_iata,
        m.movement_date,
        m.movement_hour,
        m.distinct_aircraft_count,
        m.estimated_takeoffs,
        m.estimated_landings,
        m.approaches_detected,
        m.surface_movements,
        w.weather_severity,
        w.temp_celsius,
        w.precipitation_mm,
        w.wind_speed_kmh,
        w.wind_gusts_kmh,
        w.visibility_km,
        d.is_weekend,
        d.is_religious_holiday,
        d.is_school_holiday,
        d.is_national_holiday,
        d.is_mega_event,
        d.active_events
    from movements m
    left join weather w
      on m.airport_icao = w.airport_icao
     and m.movement_date = w.observation_date
     and m.movement_hour = w.observation_hour
    left join dim_date d
      on m.movement_date = d.date_day
)

select * from joined
