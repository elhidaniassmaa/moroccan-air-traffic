-- Fact table: Air traffic density across Moroccan airspace grid (1x1 degree cells)
with flights as (
    select * from {{ ref('stg_flights') }}
),

gridded as (
    select
        event_date,
        extract(hour from position_ts) as observation_hour,
        floor(latitude) as grid_lat,
        floor(longitude) as grid_lon,
        icao24,
        baro_altitude_ft,
        velocity_kmh
    from flights
),

aggregated as (
    select
        md5(concat_ws('-', cast(event_date as varchar), cast(observation_hour as varchar), cast(grid_lat as varchar), cast(grid_lon as varchar))) as grid_hour_id,
        event_date,
        observation_hour,
        grid_lat,
        grid_lon,
        count(distinct icao24) as distinct_aircraft_count,
        count(*) as total_position_pings,
        round(cast(avg(baro_altitude_ft) as numeric), 0) as avg_altitude_ft,
        round(cast(avg(velocity_kmh) as numeric), 1) as avg_speed_kmh
    from gridded
    group by
        event_date,
        observation_hour,
        grid_lat,
        grid_lon
)

select * from aggregated
