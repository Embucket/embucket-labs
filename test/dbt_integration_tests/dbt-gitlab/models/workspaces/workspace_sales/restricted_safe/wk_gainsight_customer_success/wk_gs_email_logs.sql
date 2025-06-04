{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT
    {{ hash_sensitive_columns('email_log_v2') }}
    FROM {{ ref('email_log_v2') }}

)

SELECT *
FROM source