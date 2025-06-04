{{ config(
    materialized='ephemeral'
) }}

WITH source AS (
    
    SELECT *
    FROM {{ ref('engineering_issues') }} 
    WHERE ARRAY_CONTAINS('infradev'::VARIANT, labels)
)

SELECT *
FROM source