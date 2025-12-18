{% test validate_format(model, column_name, pattern) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} not like '{{ pattern }}'

{% endtest %}
