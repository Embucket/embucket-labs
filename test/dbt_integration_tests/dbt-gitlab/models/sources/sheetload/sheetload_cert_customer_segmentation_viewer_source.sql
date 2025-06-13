WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'cert_customer_segmentation_viewer') }}

)

SELECT *
FROM source
