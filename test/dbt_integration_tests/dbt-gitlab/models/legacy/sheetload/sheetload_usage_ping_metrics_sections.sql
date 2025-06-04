WITH source_model AS (

    SELECT *
    FROM {{ ref('sheetload_usage_ping_metrics_sections_source') }}

)

SELECT *
FROM source_model
