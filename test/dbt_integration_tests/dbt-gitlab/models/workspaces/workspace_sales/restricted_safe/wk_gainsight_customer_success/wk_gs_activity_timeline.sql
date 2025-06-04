{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT
    {{ hash_sensitive_columns('activity_timeline') }}
    FROM {{ ref('activity_timeline') }}

)

SELECT *
FROM source