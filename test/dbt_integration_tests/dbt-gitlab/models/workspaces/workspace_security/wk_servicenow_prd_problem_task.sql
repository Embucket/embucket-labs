{{ config(alias='wk_servicenow_prd_problem_task') }}

WITH source AS (
  SELECT *
  FROM {{ ref('servicenow_prd_problem_task_source') }}
)

SELECT * FROM source