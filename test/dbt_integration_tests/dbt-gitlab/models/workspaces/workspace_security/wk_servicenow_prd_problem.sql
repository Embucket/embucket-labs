{{ config(alias='wk_servicenow_prd_problem') }}

WITH source AS (
  SELECT *
  FROM {{ ref('servicenow_prd_problem_source') }}
)

SELECT * FROM source