{{ config(
    tags=["zuora_waterfall"]
) }}

WITH source AS (

  SELECT * 
  FROM {{ ref('driveload_zuora_revenue_billing_waterfall_report_with_additional_columns_source') }}

)
SELECT * 
FROM source