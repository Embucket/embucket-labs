{% macro model_count_and_group_by_date(model_name, date_column_to_group_by) %}

with model_data AS (

  SELECT
    CAST({{ date_column_to_group_by }} AS DATE) AS grouped_date,
    COUNT(*)                            AS num_rows
  FROM {{ ref(model_name) }}
  GROUP BY CAST({{ date_column_to_group_by }} AS DATE)

)

SELECT
    DISTINCT
      db.grouped_date                   AS date_day,
      IFNULL(db.num_rows, 0)            AS rowcount
FROM model_data db

{% endmacro %}