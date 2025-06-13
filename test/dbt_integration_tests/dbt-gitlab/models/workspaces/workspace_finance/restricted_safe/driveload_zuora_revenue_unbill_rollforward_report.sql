{{ config(
    tags=["zuora_waterfall"]
) }}

WITH source AS (

  SELECT * 
  FROM {{ ref('driveload_zuora_revenue_unbill_rollforward_report_source') }}

)
SELECT * 
FROM source