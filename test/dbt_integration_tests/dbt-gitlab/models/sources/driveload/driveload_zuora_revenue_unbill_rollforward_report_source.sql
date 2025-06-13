WITH source AS (

  SELECT * 
  FROM {{ source('driveload','zuora_revenue_unbill_rollforward_report') }}

)
SELECT * 
FROM source
