{% macro model_new_rows_per_day(model_name, created_column, min_value, max_value=None, where_clause=None, lag_days=None) %}

WITH dates AS (

    SELECT *
    FROM {{ ref('dim_date' )}}
    WHERE is_holiday = FALSE
    AND day_of_week NOT IN (6, 7)

), source AS (

    SELECT *
    FROM {{ ref(model_name) }}

), counts AS (

    SELECT 
      COUNT(*)                                                      AS row_count,
      DATE_TRUNC('day', {{ created_column }})                       AS the_day
    FROM source
    WHERE 
    {% if lag_days == None %}
      the_day = current_date - 1
    {% else %}
      the_day = current_date - {{ lag_days }}
    {% endif %}
      AND the_day IN (SELECT DATE_ACTUAL FROM dates)
    {% if where_clause != None %}
      AND {{ where_clause }}
    {% endif %}
    GROUP BY 2
    ORDER BY 2 DESC
    LIMIT 1

)

SELECT row_count
FROM counts
WHERE row_count < {{ min_value }} 
    {% if max_value != None %}
      OR row_count > {{ max_value }}
    {% endif %}

{% endmacro %}
