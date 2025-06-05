

WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.driveload_zuora_revenue_rc_rollforward_report_source

)
SELECT * 
FROM source