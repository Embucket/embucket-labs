WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'cert_product_geo_sql') }}

)

SELECT *
FROM source
