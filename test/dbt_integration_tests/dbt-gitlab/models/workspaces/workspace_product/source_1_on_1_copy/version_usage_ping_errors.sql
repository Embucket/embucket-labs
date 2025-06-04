WITH source AS (

    SELECT *
    FROM {{ ref('version_usage_ping_errors_source') }}

)

SELECT *
FROM source
