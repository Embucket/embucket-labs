WITH bamboohr AS (
  
  SELECT *
  FROM {{ ref('bamboohr_job_info_source') }}

),

workday AS (

  SELECT *
  FROM {{ ref('workday_job_info_source') }}
),

map AS (

  SELECT *
  FROM {{ ref('map_employee_id') }}
),

unioned AS (
SELECT
  map.wk_employee_id AS employee_id,
  bamboohr.job_title,
  bamboohr.effective_date,
  bamboohr.department,
  bamboohr.division,
  bamboohr.entity,
  bamboohr.reports_to,
  bamboohr.uploaded_at,
  ROW_NUMBER() OVER (PARTITION BY map.wk_employee_id, bamboohr.effective_date ORDER BY bamboohr.job_id ASC) AS job_sequence,
  'bamboohr' AS source_system
FROM bamboohr
INNER JOIN map
  ON bamboohr.employee_id = map.bhr_employee_id

UNION 

SELECT
  employee_id,
  job_title,
  effective_date,
  department,
  division,
  entity,
  reports_to,
  uploaded_at,
  ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY initiated_at ASC) AS job_sequence,
  'workday' AS source_system
FROM workday
),

filtered AS (

  {{ gitlab_snowflake.workday_bamboohr_blending_filter('unioned', ['employee_id', 'effective_date','job_sequence'],filter_date='effective_date') }}
  
)

SELECT *
FROM filtered