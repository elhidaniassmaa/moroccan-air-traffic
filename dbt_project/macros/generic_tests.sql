{% test assert_valid_latitude(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} < 21.0 or {{ column_name }} > 36.0
{% endtest %}

{% test assert_plausible_velocity_kmh(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} < 0 or {{ column_name }} > 1200.0
{% endtest %}
