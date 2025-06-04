WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'cert_pricing_customer_discount_dashboard') }}

)

SELECT *
FROM source
