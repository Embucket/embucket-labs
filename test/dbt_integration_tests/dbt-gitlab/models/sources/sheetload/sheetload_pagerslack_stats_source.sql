WITH source AS (

    SELECT *
    FROM {{ source('sheetload','pagerslack_stats') }}

),renamed AS (

    SELECT
        attempts::NUMBER                AS attempts,
        true::BOOLEAN         AS weekend_pinged,
        true::BOOLEAN            AS unavailable,
        incident_url::VARCHAR           AS incident_url,
        reported_by::VARCHAR            AS reported_by,
        reported_at::TIMESTAMP          AS reported_at,
        time_to_response::FLOAT         AS time_to_response,
        time_at_response::TIMESTAMP     AS time_at_response
    FROM source
)

SELECT *
FROM renamed
