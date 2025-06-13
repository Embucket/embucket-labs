WITH source AS (

  SELECT * 
  FROM {{ source('sheetload','map_ramp_deals') }}

)
SELECT * 
FROM source
