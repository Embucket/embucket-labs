WITH bamboohr AS (

    SELECT *
    FROM {{ ref('employee_directory') }}
  
), source AS (

    SELECT *
    FROM {{ ref('sheetload_osat_source') }}

), final AS (

    SELECT
      source.completed_date,
      COALESCE(DATE_TRUNC(month, source.hire_date), DATE_TRUNC(month, bamboohr.hire_date)) AS hire_month,
      source.division,
      source.satisfaction_score,
      source.recommend_to_friend,
      source.buddy_experience_score
    FROM source
    LEFT JOIN bamboohr
      ON source.employee_name = CONCAT(bamboohr.first_name, ' ', bamboohr.last_name)
    WHERE source.completed_date IS NOT NULL

)
SELECT *
FROM final
