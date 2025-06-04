{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT
    {{ hash_sensitive_columns('opt_out_emails') }}
    FROM {{ ref('opt_out_emails') }}

)

SELECT *
FROM source