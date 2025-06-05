

WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.driveload_zuora_revenue_waterfall_report_with_wf_type_adjustments_source

)
SELECT * 
FROM source