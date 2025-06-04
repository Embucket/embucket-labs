
WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'cert_customer_segmentation_dashboard') }}

)

SELECT *
FROM source
