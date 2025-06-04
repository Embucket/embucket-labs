WITH source AS (

    SELECT *
    FROM {{ ref('version_usage_ping_metadata_source') }}

)

SELECT *
FROM source
