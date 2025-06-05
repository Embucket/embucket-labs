

WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.driveload_zuora_revenue_unbill_rollforward_report_source

)
SELECT * 
FROM source