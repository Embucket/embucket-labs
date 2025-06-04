{{ config(
    tags=["zuora_waterfall"]
) }}

WITH source AS (

  SELECT * 
  FROM {{ ref('driveload_zuora_revenue_unreleased_pob_report_source') }}

)
SELECT * 
FROM source