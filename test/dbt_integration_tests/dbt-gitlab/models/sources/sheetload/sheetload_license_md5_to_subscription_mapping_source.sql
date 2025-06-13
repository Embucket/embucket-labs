WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'license_md5_to_subscription_mapping') }}

)

SELECT *
FROM source
