WITH source AS (

  SELECT * 
  FROM {{ source('sheetload','xactly_credit_sheetload') }}

)
SELECT * 
FROM source
