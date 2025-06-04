{{ config({
    "schema": "legacy",
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
    })
}}

WITH source AS (

  SELECT *
  FROM {{ ref('blended_bonus_source') }}

), current_division_department_mapping AS (

    SELECT * 
    FROM {{ ref('bamboohr_job_info_current_division_base') }}

), filtered AS (

    SELECT
      source.*,
      department,
      division_mapped_current AS division
    FROM source
    LEFT JOIN current_division_department_mapping
      ON source.employee_id = current_division_department_mapping.employee_id
      AND source.bonus_date BETWEEN current_division_department_mapping.effective_date 
                            AND COALESCE(current_division_department_mapping.effective_end_date::DATE, {{max_date_in_bamboo_analyses()}})
    WHERE source.bonus_type = 'Discretionary Bonus'

)

SELECT *
FROM filtered
