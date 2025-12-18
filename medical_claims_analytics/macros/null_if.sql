{% macro null_if(col) %}
        NULLIF(
            NULLIF({{ col }}, ''),
            'null'
        ) 
{% endmacro %}
