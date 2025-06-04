{{ config(alias='wk_servicenow_prd_incident_task') }}

WITH source AS (
  SELECT *
  FROM {{ ref('servicenow_prd_incident_task_source') }}
)

SELECT * FROM source