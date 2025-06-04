WITH bamboohr AS (
  
  SELECT *
  FROM {{ ref('bamboohr_compensation_source') }}

),

workday AS (

  SELECT *
  FROM {{ ref('workday_compensation_source') }}
),

map AS (

  SELECT *
  FROM {{ ref('map_employee_id') }}
),


unioned AS (
SELECT
  map.wk_employee_id AS employee_id,
  bamboohr.effective_date,
  bamboohr.compensation_type,
  bamboohr.compensation_change_reason,
  bamboohr.pay_rate,
  bamboohr.compensation_value,
  bamboohr.compensation_currency,
  bamboohr.uploaded_at,
  ROW_NUMBER() OVER (PARTITION BY map.wk_employee_id, bamboohr.effective_date ORDER BY bamboohr.compensation_update_id ASC) AS compensation_sequence,
  'bamboohr' AS source_system
FROM bamboohr
INNER JOIN map
  ON bamboohr.employee_id = map.bhr_employee_id

UNION 

SELECT
  employee_id,
  effective_date,
  compensation_type,
  compensation_change_reason,
  pay_rate,
  per_pay_period_amount,
  compensation_currency,
  uploaded_at,
  ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY initiated_at ASC) AS compensation_sequence,
  'workday' AS source_system
FROM workday
),

filtered AS (

  {{ gitlab_snowflake.workday_bamboohr_blending_filter('unioned', ['employee_id', 'effective_date','compensation_change_reason','compensation_sequence']) }}
  
)

SELECT *
FROM filtered
