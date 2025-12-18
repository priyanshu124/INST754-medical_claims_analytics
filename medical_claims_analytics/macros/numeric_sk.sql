{% macro numeric_sk(columns) %}
    -- Returns a stable numeric surrogate key from a string column

    {% if columns | length == 1 %}
        {% set hash_input = columns[0] %}
    {% else %}
        {% set hash_input = "CONCAT(" ~ columns | join(', ') ~ ")" %}
    {% endif %}

    ABS(CAST(CAST(HASHBYTES('SHA1', {{ hash_input }}) AS BINARY(4)) AS BIGINT))
{% endmacro %}
