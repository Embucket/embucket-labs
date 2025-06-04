{{ config(materialized='view') }}

WITH final AS (

SELECT 
  opportunity_id,
  user_or_group_id,
  opportunity_access_level,
  row_cause

FROM {{ ref('sfdc_opportunity_share_source') }}
WHERE 
    is_deleted = false

)

SELECT * 
FROM final