{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT
    {{ hash_sensitive_columns('advanced_outreach_participants') }}
    FROM {{ ref('advanced_outreach_participants') }}

)

SELECT *
FROM source