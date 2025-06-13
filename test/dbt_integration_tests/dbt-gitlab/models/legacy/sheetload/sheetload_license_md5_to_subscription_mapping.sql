WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_license_md5_to_subscription_mapping_source') }}

)

SELECT *
FROM source
