{% test validate_date(model, column_name, min_date='1900-01-01', start_date_column=None) %}

select *
from {{ model }}
where
{{ column_name }} is not null  -- ALLOW NULLS   
    and (
        {{ column_name }} < '{{ min_date }}'  ---- Date is unrealistically old
        {% if start_date_column %}
            or {{ column_name }} < {{ start_date_column }}  -- END DATE MUST BE AFTER OR EQUAL TO START DATE
        {% else %} 
            or {{ column_name }} > CAST(GETDATE() AS DATE) -- DATE MUST BE IN THE PAST
        {% endif %}
        
    )
{% endtest %}
