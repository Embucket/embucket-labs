WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.zuora_revenue_unbill_rollforward_report

)
SELECT * 
FROM source