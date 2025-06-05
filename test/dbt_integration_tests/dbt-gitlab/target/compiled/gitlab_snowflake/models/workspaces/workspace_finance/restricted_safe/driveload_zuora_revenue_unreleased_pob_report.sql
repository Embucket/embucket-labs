

WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.driveload_zuora_revenue_unreleased_pob_report_source

)
SELECT * 
FROM source