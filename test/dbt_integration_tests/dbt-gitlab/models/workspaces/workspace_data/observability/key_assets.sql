WITH source AS (

    SELECT *
    FROM {{ ref('key_assets_source') }}

)

SELECT *
FROM source
