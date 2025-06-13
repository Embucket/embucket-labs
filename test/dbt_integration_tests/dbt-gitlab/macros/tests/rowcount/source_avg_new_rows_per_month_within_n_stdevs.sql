{% macro source_avg_new_rows_per_month_within_n_stdevs(source_name, table, created_column, nr_std_devs=1, where_clause=None) %}

WITH source AS (

    SELECT *
    FROM {{ source(source_name, table)}}

), counts AS (

    SELECT 
      TRUNC({{ created_column }}, 'Month')                                                AS line_created_month,
      COUNT(*)                                                                            AS new_records,
      AVG(new_records) OVER()                                                             AS average_new_records_per_month, 
      STDDEV(new_records) OVER()                                                          AS std_dev_new_records_per_month,
      average_new_records_per_month + ({{ nr_std_devs }} * std_dev_new_records_per_month) AS monthly_new_records_threshold
    FROM source
    {% if where_clause != None %}
    WHERE {{ where_clause }}
    {% endif %}
    GROUP BY 1
    ORDER BY 1

)

SELECT new_records
FROM counts
WHERE  new_records > monthly_new_records_threshold

{% endmacro %}
