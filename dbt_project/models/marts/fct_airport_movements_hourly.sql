{{
    config(
        materialized='incremental',
        unique_key='movement_hour_id',
        incremental_strategy='delete+insert'
    )
}}

-- Incremental fact table: airport movements by hour
with flights_near as (
    select * from {{ ref('int_flights_near_airport') }}
    {% if is_incremental() %}
    where event_date >= (select coalesce(max(movement_date), date '2024-01-01') - interval '1 day' from {{ this }})
    {% endif %}
),

phases as (
    select
        icao24,
        position_ts,
        computed_flight_phase
    from {{ ref('int_flight_phases') }}
),

joined as (
    select
        f.airport_icao,
        f.airport_iata,
        f.event_date as movement_date,
        extract(hour from f.position_ts) as movement_hour,
        f.icao24,
        f.callsign,
        p.computed_flight_phase,
        f.distance_to_airport_km
    from flights_near f
    left join phases p
      on f.icao24 = p.icao24
     and f.position_ts = p.position_ts
),

aggregated as (
    select
        md5(concat_ws('-', airport_icao, cast(movement_date as varchar), cast(movement_hour as varchar))) as movement_hour_id,
        airport_icao,
        airport_iata,
        movement_date,
        movement_hour,
        count(distinct icao24) as distinct_aircraft_count,
        count(distinct case when computed_flight_phase = 'takeoff' then icao24 end) as estimated_takeoffs,
        count(distinct case when computed_flight_phase = 'landing' then icao24 end) as estimated_landings,
        count(distinct case when computed_flight_phase = 'approach' then icao24 end) as approaches_detected,
        count(distinct case when computed_flight_phase = 'on_ground' then icao24 end) as surface_movements,
        round(avg(distance_to_airport_km), 2) as avg_distance_km,
        current_timestamp as updated_at
    from joined
    group by
        airport_icao,
        airport_iata,
        movement_date,
        movement_hour
)

select * from aggregated
