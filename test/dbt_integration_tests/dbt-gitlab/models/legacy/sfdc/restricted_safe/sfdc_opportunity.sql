WITH base AS (

  SELECT *
    FROM {{ref('sfdc_opportunity_source_legacy')}} 
    WHERE account_id IS NOT NULL
    AND is_deleted = FALSE

)

SELECT *
FROM base