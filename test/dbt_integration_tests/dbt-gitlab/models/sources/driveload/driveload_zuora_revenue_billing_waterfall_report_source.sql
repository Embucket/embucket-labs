WITH source AS (

  SELECT * 
  FROM {{ source('driveload','zuora_revenue_billing_waterfall_report') }}

)
SELECT * 
FROM source