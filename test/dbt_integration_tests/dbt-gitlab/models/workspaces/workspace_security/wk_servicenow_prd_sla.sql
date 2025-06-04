{{ config(alias='wk_servicenow_prd_sla') }}

WITH source AS (
  SELECT *
  FROM {{ ref('servicenow_prd_sla_source') }}
)

SELECT * FROM source