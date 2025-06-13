{% macro schema_union_limit(schema_part, table_name, column_name, day_limit=30, database_name=none, boolean_filter_statement=none, excluded_col=[]) %}

WITH base_union AS (

  {{ schema_union_all(schema_part, table_name, database_name=database_name, day_limit=day_limit, boolean_filter_statement=boolean_filter_statement, excluded_col=excluded_col) }}

) 

SELECT *
FROM base_union
WHERE {{ column_name }} >= dateadd('day', -{{ day_limit }}, CURRENT_DATE())

{% endmacro %}
