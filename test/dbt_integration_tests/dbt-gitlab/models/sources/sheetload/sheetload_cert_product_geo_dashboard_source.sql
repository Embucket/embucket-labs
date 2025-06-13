WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'cert_product_geo_dashboard') }}

)

SELECT *
FROM source
