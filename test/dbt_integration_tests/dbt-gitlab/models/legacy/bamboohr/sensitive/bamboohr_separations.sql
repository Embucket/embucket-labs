WITH dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), separations AS (

    SELECT
      employee_number,
      employee_id,
      hire_date,
      date_actual                       AS separation_date, 
      DATE_TRUNC(month, date_actual)    AS separation_month, 
      division_mapped_current           AS division, 
      department_modified               AS department,
      job_title                         AS job_title
    FROM {{ ref('employee_directory_intermediate') }}
    WHERE is_termination_date = TRUE
      AND date_actual>='2020-02-01'

), separation_type AS (

    SELECT *
    FROM {{ ref('blended_employment_status_source') }}
    WHERE LOWER(employment_status) = 'terminated'

), eeoc AS (

    SELECT *
    FROM {{ ref('bamboohr_id_employee_number_mapping') }}

), final AS (

    SELECT 
      dim_date.fiscal_year,
      separations.*,
      termination_type,
      eeoc.gender,
      eeoc.ethnicity,
      eeoc.region
    FROM separations
    LEFT JOIN dim_date
      ON separations.separation_date = dim_date.date_actual
    LEFT JOIN separation_type 
      ON separations.employee_id = separation_type.employee_id
      AND separations.separation_date = separation_type.effective_date
    LEFT JOIN eeoc 
      ON separations.employee_id = eeoc.employee_id

)

SELECT *
FROM final
