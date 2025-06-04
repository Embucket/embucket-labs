{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT
    {{ hash_sensitive_columns('companies_and_ebrs') }}
    FROM {{ ref('companies_and_ebrs') }}

)

SELECT *
FROM source