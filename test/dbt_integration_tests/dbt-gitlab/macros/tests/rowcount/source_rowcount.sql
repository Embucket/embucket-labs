{% macro source_rowcount(source_name, table, count, where_clause=None) %}

WITH source AS (

    SELECT *
    FROM {{ source(source_name, table) }}

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
