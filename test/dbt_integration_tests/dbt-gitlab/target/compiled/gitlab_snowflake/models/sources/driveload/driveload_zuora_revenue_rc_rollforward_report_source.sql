WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.zuora_revenue_rc_rollforward_report

)
SELECT * 
FROM source