WITH source AS (

  SELECT * 
  FROM {{ source('driveload','zuora_revenue_revenue_waterfall_report') }}

)
SELECT * 
FROM source