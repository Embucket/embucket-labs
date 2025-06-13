WITH bamboohr AS (
  
  SELECT *
  FROM {{ ref('bamboohr_directory_source') }}

),

workday AS (

  SELECT *
  FROM {{ ref('workday_directory_source') }} 
),

map AS (

  SELECT *
  FROM {{ ref('map_employee_id') }}
),

unioned AS (
SELECT
  map.wk_employee_id AS employee_id,
  bamboohr.work_email,
  bamboohr.full_name,
  bamboohr.job_title,
  bamboohr.supervisor,
  bamboohr.uploaded_at,
  'bamboohr' AS source_system
FROM bamboohr
INNER JOIN map
  ON bamboohr.employee_id = map.bhr_employee_id

UNION 

SELECT
  employee_id,
  work_email,
  full_name,
  job_title,
  supervisor,
  uploaded_at,
  'workday' AS source_system
FROM workday
),

filtered AS (

  {{ gitlab_snowflake.workday_bamboohr_blending_filter('unioned', ['employee_id', 'DATE_TRUNC(\'day\',uploaded_at)'],'uploaded_at DESC') }}
  
)

SELECT *
FROM filtered
