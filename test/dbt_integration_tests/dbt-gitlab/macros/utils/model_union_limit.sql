{% macro model_union_limit(model_name, column_name, day_limit=30) %}

WITH base AS (
    
    SELECT *
    FROM {{ ref(model_name) }}

) 

SELECT *
FROM base
WHERE {{ column_name }} >= dateadd('day', -{{ day_limit }}, CURRENT_DATE())

{% endmacro %}
