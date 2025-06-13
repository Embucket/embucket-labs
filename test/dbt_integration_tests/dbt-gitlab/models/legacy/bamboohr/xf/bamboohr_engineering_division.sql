{{ config({
    "schema": "legacy",
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
    })
}}

WITH employees AS (

    SELECT *
    FROM {{ ref('employee_directory_analysis') }}

), bamboohr_engineering_division_mapping AS (

    SELECT
        DISTINCT LOWER(group_name) AS group_name,
        LOWER(stage_section) AS section_name,
        LOWER(stage_display_name) AS stage_name
    FROM {{ ref('stages_groups_yaml_source') }}
    WHERE snapshot_date = (SELECT max(snapshot_date) FROM {{ ref('stages_groups_yaml_source') }})

), engineering_employees AS (

    SELECT
      date_actual,
      employee_id,
      full_name,
      job_title                     AS job_title,
      TRIM(SUBSTRING(LOWER(TRIM(VALUE::VARCHAR)),charindex(':',LOWER(TRIM(VALUE::VARCHAR)))+1,100))   AS job_title_speciality,
      reports_to,
      layers,
      department,
      work_email
    FROM employees,
    LATERAL FLATTEN(INPUT=>SPLIT(COALESCE(REPLACE(jobtitle_speciality,'&',','),''), ','))
    WHERE division = 'Engineering'
      AND date_actual >= '2020-01-01'

), engineering_employee_attributes AS (
    
    SELECT 
      engineering_employees.date_actual,
      engineering_employees.employee_id,
      engineering_employees.full_name,
      engineering_employees.job_title,
      bamboohr_engineering_division_mapping.section_name,
      bamboohr_engineering_division_mapping.stage_name,
      engineering_employees.job_title_speciality,
      CASE 
        WHEN engineering_employees.employee_id IN (11480,11522,11500,11555,11013,11008,10979,10442,10072,11491) 
            OR LOWER(engineering_employees.job_title) LIKE '%backend%' 
          THEN 'backend'
        WHEN LOWER(engineering_employees.job_title) LIKE '%fullstack%'
          THEN 'fullstack'
        WHEN LOWER(engineering_employees.job_title) LIKE '%frontend%'
          THEN 'frontend'
        ELSE NULL END               AS technology_group,
      engineering_employees.department,
      engineering_employees.work_email,
      engineering_employees.reports_to
    FROM engineering_employees
    LEFT JOIN bamboohr_engineering_division_mapping
      ON bamboohr_engineering_division_mapping.group_name = engineering_employees.job_title_speciality 

)

SELECT *
FROM engineering_employee_attributes
