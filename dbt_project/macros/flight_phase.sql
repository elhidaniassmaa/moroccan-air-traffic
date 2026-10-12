{% macro flight_phase(on_ground, baro_altitude_m, velocity_ms, vertical_rate_ms) %}
    {#
        Heuristic classification of flight phase:
        - on_ground
        - landing
        - approach
        - takeoff
        - climb
        - descent
        - cruise
        - unknown
    #}
    case
        when {{ on_ground }} is true then 'on_ground'
        when coalesce({{ baro_altitude_m }}, 0) < 300
             and coalesce({{ vertical_rate_ms }}, 0) < 0
             and coalesce({{ velocity_ms }}, 0) < 80 then 'landing'
        when coalesce({{ baro_altitude_m }}, 0) < 2000
             and coalesce({{ vertical_rate_ms }}, 0) < -1.0 then 'approach'
        when coalesce({{ baro_altitude_m }}, 0) < 2000
             and coalesce({{ vertical_rate_ms }}, 0) > 3.0
             and coalesce({{ velocity_ms }}, 0) > 30.0 then 'takeoff'
        when coalesce({{ baro_altitude_m }}, 0) < 8000
             and coalesce({{ vertical_rate_ms }}, 0) > 2.0 then 'climb'
        when coalesce({{ baro_altitude_m }}, 0) < 8000
             and coalesce({{ vertical_rate_ms }}, 0) < -2.0 then 'descent'
        when coalesce({{ baro_altitude_m }}, 0) >= 8000
             and abs(coalesce({{ vertical_rate_ms }}, 0)) <= 2.0 then 'cruise'
        when coalesce({{ baro_altitude_m }}, 0) >= 8000
             and coalesce({{ vertical_rate_ms }}, 0) > 2.0 then 'climb'
        when coalesce({{ baro_altitude_m }}, 0) >= 8000
             and coalesce({{ vertical_rate_ms }}, 0) < -2.0 then 'descent'
        else 'unknown'
    end
{% endmacro %}
