WITH source AS (

  SELECT * 
  FROM {{ source('driveload','zuora_revenue_rc_rollforward_report') }}

)
SELECT * 
FROM source