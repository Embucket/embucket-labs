

WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.driveload_zuora_revenue_billing_waterfall_report_with_additional_columns_source

)
SELECT * 
FROM source