{% macro model_rowcount(model_name, count, where_clause=None) %}

WITH source AS (

    SELECT *
    FROM {{ ref(model_name) }}

), counts AS (

    SELECT count(*) AS row_count
    FROM source
    {% if where_clause != None %}
    WHERE {{ where_clause }}
    {% endif %}

)

SELECT row_count
FROM counts
WHERE row_count < {{ count }}

{% endmacro %}
