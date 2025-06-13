{{ config(materialized='view') }}

WITH final AS (

SELECT 
  account_id,
  user_or_group_id,
  account_access_level,
  row_cause

FROM {{ ref('sfdc_account_share_source') }}
WHERE 
    is_deleted = false

)

SELECT * 
FROM final