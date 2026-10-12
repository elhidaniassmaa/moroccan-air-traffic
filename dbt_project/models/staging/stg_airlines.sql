-- Staging model for airlines seed
with source as (
    select * from {{ ref('airlines') }}
),

renamed as (
    select
        callsign_prefix,
        airline_icao,
        airline_iata,
        airline_name,
        country,
        coalesce(is_moroccan, false) as is_moroccan
    from source
)

select * from renamed
