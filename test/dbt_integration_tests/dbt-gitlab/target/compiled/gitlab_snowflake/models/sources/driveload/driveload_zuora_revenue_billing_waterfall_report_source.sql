WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.zuora_revenue_billing_waterfall_report

)
SELECT * 
FROM source