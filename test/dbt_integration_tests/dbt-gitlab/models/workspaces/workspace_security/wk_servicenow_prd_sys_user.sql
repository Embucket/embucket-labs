{{ config(alias='wk_servicenow_prd_sys_user') }}

WITH source AS (
  SELECT *
  FROM {{ ref('servicenow_prd_sys_user_source') }}
)

SELECT * FROM source