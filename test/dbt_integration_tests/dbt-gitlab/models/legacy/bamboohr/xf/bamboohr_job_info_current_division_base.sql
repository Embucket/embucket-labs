{{ config({
    "schema": "legacy",
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
    })
}}

WITH job_info AS (

    SELECT *
    FROM {{ ref ('bamboohr_job_info') }}

), bamboo_mapping AS (

    SELECT *
    FROM {{ ref ('bamboohr_id_employee_number_mapping') }}

), job_role AS (

    SELECT *
    FROM {{ ref ('bamboohr_job_role') }}

), department_name_changes AS (

    SELECT  
      TRIM(old_department_name) AS old_department_name,
      TRIM(new_department_name) AS new_department_name,
      change_effective_date
    FROM {{ref ('department_name_changes')}}
    WHERE change_effective_date < CURRENT_DATE

), current_division_department_mapping AS (

    SELECT DISTINCT 
      division, 
      department,
      MAX(effective_date) AS max_effective_date
    FROM bamboo_mapping
    LEFT JOIN job_info
      ON job_info.employee_id = bamboo_mapping.employee_id
    WHERE CURRENT_DATE() BETWEEN effective_date AND COALESCE(effective_end_date, CURRENT_DATE())
      AND bamboo_mapping.termination_date IS NULL
    GROUP BY 1,2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY department ORDER BY max_effective_date DESC) =1 
    --to account for individuals that have not been transistioned to new division  

)

SELECT 
  job_info.*, 
  IFF(job_info.department = 'Meltano', 'Engineering',
      COALESCE(current_division_department_mapping.division, job_info.division))               AS division_mapped_current,
  {{bamboohr_division_grouping(division=
    'COALESCE(current_division_department_mapping.division, job_info.division)')}}             AS division_grouping,      
  COALESCE(department_name_changes.new_department_name, job_info.department)                   AS department_modified,  
  {{bamboohr_department_grouping(department='department_modified')}}                           AS department_grouping,  
  bamboo_mapping.termination_date,
  bamboo_mapping.first_inactive_date  
FROM bamboo_mapping
LEFT JOIN job_info 
  ON job_info.employee_id = bamboo_mapping.employee_id
LEFT JOIN department_name_changes
  ON job_info.department = department_name_changes.old_department_name  
LEFT JOIN current_division_department_mapping
  ON current_division_department_mapping.department = COALESCE(department_name_changes.new_department_name, job_info.department)

