-- Dimension: Airline
with airlines as (
    select * from {{ ref('stg_airlines') }}
)

select
    callsign_prefix,
    airline_icao,
    airline_iata,
    airline_name,
    country,
    is_moroccan
from airlines
