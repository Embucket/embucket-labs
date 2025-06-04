WITH source AS (

    SELECT *
    FROM {{ ref('thanos_daily_slas_source') }}
    WHERE is_success = true
)
SELECT *
FROM source
