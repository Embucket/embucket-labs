{{ config(
    tags=["product"]
) }}

WITH source AS (

    SELECT DISTINCT stage_name
    FROM {{ ref('sheetload_usage_ping_metrics_sections') }}
    WHERE is_smau

)

SELECT *
FROM source
