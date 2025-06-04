WITH bamboohr AS (
  
  SELECT *
  FROM {{ ref('bamboohr_currency_conversion_source') }}

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
  bamboohr.currency_conversion_factor,
  SPLIT_PART(bamboohr.local_annual_salary, ' ', 1)::FLOAT AS local_annual_salary,
  bamboohr.annual_local_currency_code,
  bamboohr.annual_amount_usd_value,
  bamboohr.annual_local_usd_code,
  bamboohr.uploaded_at,
  ROW_NUMBER() OVER (PARTITION BY map.wk_employee_id, bamboohr.effective_date ORDER BY bamboohr.conversion_id ASC) AS currency_conversion_sequence,
  'bamboohr' AS source_system
FROM bamboohr
INNER JOIN map
  ON bamboohr.employee_id = map.bhr_employee_id

UNION 

SELECT
  employee_id,
  DATE_TRUNC('month', initiated_at)::DATE AS effective_date,
  conversion_rate_local_to_usd,
  compensation_value,
  compensation_currency, 
  compensation_value_usd,
  compensation_currency_usd,
  uploaded_at,
  ROW_NUMBER() OVER (PARTITION BY employee_id, DATE_TRUNC('month', initiated_at) ORDER BY initiated_at ASC) AS currency_conversion_sequence,
  'workday' AS source_system
FROM workday
),

filtered AS (

  {{ gitlab_snowflake.workday_bamboohr_blending_filter('unioned', ['employee_id', 'effective_date','currency_conversion_factor','local_annual_salary','currency_conversion_sequence']) }}
  
)

SELECT *
FROM filtered