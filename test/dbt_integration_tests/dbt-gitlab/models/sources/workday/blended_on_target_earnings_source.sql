WITH bamboohr AS (
  
  SELECT *
  FROM {{ ref('bamboohr_ote_source') }}

),

workday AS (

  SELECT *
  FROM {{ ref('workday_on_target_earnings_source') }}
),

map AS (

  SELECT *
  FROM {{ ref('map_employee_id') }}
),

unioned AS (
SELECT
  map.wk_employee_id AS employee_id,
  bamboohr.effective_date,
  bamboohr.variable_pay,
  bamboohr.annual_amount_local,
  bamboohr.annual_amount_usd_value,
  bamboohr.ote_local,
  bamboohr.ote_usd,
  bamboohr.ote_type,
  bamboohr.annual_amount_local_currency_code,
  bamboohr.ote_local_currency_code,
  uploaded_at,
  ROW_NUMBER() OVER (PARTITION BY map.wk_employee_id, bamboohr.effective_date ORDER BY bamboohr.target_earnings_update_id ASC) AS target_earnings_sequence,
  'bamboohr' AS source_system
FROM bamboohr
INNER JOIN map
  ON bamboohr.employee_id = map.bhr_employee_id

UNION 

SELECT
  employee_id,
  effective_date,
  'Yes' AS variable_pay,
  annual_amount_local,
  annual_amount_usd_value,
  ote_local,
  ote_usd,
  ote_type,
  annual_amount_local_currency_code,
  ote_local_currency_code,
  uploaded_at,
  ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY initiated_at,workday_id ASC) AS target_earnings_sequence,
  'workday' AS source_system
FROM workday
),

filtered AS (

  {{ gitlab_snowflake.workday_bamboohr_blending_filter('unioned', ['employee_id', 'effective_date','target_earnings_sequence']) }}
  
)

SELECT *
FROM filtered