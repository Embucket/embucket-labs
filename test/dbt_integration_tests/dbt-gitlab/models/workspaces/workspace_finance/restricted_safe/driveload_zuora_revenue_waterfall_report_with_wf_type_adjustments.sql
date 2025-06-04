{{ config(
    tags=["zuora_waterfall"]
) }}

WITH source AS (

  SELECT * 
  FROM {{ ref('driveload_zuora_revenue_waterfall_report_with_wf_type_adjustments_source') }}

)
SELECT * 
FROM source