WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'product_maturity_historical') }}

)

SELECT * 
FROM source
