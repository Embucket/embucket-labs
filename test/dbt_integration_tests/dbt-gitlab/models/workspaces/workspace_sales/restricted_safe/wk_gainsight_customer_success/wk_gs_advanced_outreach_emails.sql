{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT
    {{ hash_sensitive_columns('advanced_outreach_emails') }}
    FROM {{ ref('advanced_outreach_emails') }}

)

SELECT *
FROM source