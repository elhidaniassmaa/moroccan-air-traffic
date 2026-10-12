{% snapshot snap_dim_airline %}

{{
    config(
      target_schema='snapshots',
      unique_key='callsign_prefix',
      strategy='check',
      check_cols=['airline_name', 'country', 'is_moroccan'],
      invalidate_hard_deletes=True
    )
}}

select
    callsign_prefix,
    airline_icao,
    airline_iata,
    airline_name,
    country,
    is_moroccan
from {{ ref('stg_airlines') }}

{% endsnapshot %}
