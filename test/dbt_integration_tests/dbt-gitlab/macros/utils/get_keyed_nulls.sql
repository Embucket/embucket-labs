{% macro get_keyed_nulls(columns) %}

  COALESCE({{columns}}, MD5(-1)) 

{% endmacro %}
