{{ config(alias='wk_servicenow_prd_incident') }}

WITH source AS (
  SELECT *
  FROM {{ ref('servicenow_prd_incident_source') }}
)

SELECT * FROM source