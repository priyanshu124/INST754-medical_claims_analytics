{% macro null_if_all_cols(relation) %}
{# 
  Loops through all columns in the given table or model and applies the `null_if` macro to each column.
  Returns a comma-separated list of columns suitable for a SELECT statement.
#}
{% set columns = adapter.get_columns_in_relation(relation) %}

{% for col in columns %}
    {{ null_if(col.name) }} AS {{ col.name }}{% if not loop.last %},{% endif %}
{% endfor %}
{% endmacro %}
