WITH bamboohr AS (
  
  SELECT *
  FROM {{ ref('bamboohr_id_employee_number_mapping_source') }}

),

workday AS (

  SELECT *
  FROM {{ ref('workday_employee_mapping_source') }} 
  
),

unioned AS (
SELECT
  employee_number,
  employee_number AS employee_id,
  first_name,
  last_name,
  hire_date,
  termination_date,
  termination_date + 1 AS first_inactive_date,
  greenhouse_candidate_id,
  cost_center,
  gitlab_username,
  jobtitle_speciality_single_select,
  jobtitle_speciality_multi_select,
  locality,
  nationality,
  gender_dropdown,
  region,
  job_role,
  sales_geo_differential,
  date_of_birth,
  employee_status_date,
  employment_history_status,
  ethnicity,
  gender,
  country,
  age,
  job_grade,
  pay_frequency,
  uploaded_at,
  uploaded_row_number_desc,
  'bamboohr' AS source_system
FROM bamboohr

UNION 

SELECT
  employee_id AS employee_number,
  employee_id,
  first_name,
  last_name,
  hire_date,
  termination_date,
  termination_date + 1 AS first_inactive_date,
  greenhouse_candidate_id,
  cost_center,
  gitlab_username,
  jobtitle_speciality_single_select,
  REGEXP_REPLACE(jobtitle_speciality_multi_select,'; ',',') AS jobtitle_speciality_multi_select,
  locality,
  nationality,
  gender_dropdown,
  region,
  job_role,
  sales_geo_differential,
  date_of_birth,
  employee_status_date,
  employment_history_status,
  ethnicity,
  gender,
  country,
  age,
  job_grade::VARCHAR AS job_grade, -- BambooHR data is a text filed.
  pay_frequency,
  uploaded_at,
  DENSE_RANK() OVER (ORDER BY DATE_TRUNC('day',uploaded_at) DESC) AS uploaded_row_number_desc,
  'workday' AS source_system
FROM workday
),

filtered AS (

  {{ gitlab_snowflake.workday_bamboohr_blending_filter('unioned', ['employee_id','DATE_TRUNC(\'day\',uploaded_at)']) }}
  
)

SELECT *
FROM filtered
WHERE employee_id NOT IN (SELECT employee_id FROM {{ ref('test_employee_ids' )}})