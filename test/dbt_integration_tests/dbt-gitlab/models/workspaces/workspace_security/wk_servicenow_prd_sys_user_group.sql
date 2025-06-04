{{ config(alias='wk_servicenow_prd_sys_user_group') }}

WITH source AS (
  SELECT *
  FROM {{ ref('servicenow_prd_sys_user_group_source') }}
)

SELECT * FROM source