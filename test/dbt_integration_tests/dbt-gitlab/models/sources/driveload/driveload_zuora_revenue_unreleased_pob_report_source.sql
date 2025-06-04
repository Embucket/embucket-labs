WITH source AS (

  SELECT * 
  FROM {{ source('driveload','zuora_revenue_unreleased_pob_report') }}

)
SELECT * 
FROM source
