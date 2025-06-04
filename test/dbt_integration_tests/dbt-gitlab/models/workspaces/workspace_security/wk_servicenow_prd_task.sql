{{ config(alias='wk_servicenow_prd_task') }}

WITH source AS (
  SELECT *
  FROM {{ ref('servicenow_prd_task_source') }}
)

SELECT * FROM source