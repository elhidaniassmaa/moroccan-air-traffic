{% macro haversine_km(lat1, lon1, lat2, lon2) %}
    {#
        Calculates great-circle distance between two points in km.
        Formula: 2 * R * asin(sqrt(sin^2(dlat/2) + cos(lat1)*cos(lat2)*sin^2(dlon/2)))
        R = 6371 km
    #}
    2 * 6371.0 * asin(
        sqrt(
            power(sin(radians({{ lat2 }} - {{ lat1 }}) / 2.0), 2)
            + cos(radians({{ lat1 }})) * cos(radians({{ lat2 }}))
            * power(sin(radians({{ lon2 }} - {{ lon1 }}) / 2.0), 2)
        )
    )
{% endmacro %}
