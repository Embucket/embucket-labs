WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_product_maturity_historical_source') }}

)

SELECT *
FROM source
