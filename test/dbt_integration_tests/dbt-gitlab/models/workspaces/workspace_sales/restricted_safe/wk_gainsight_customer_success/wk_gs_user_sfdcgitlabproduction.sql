{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT
    {{ hash_sensitive_columns('user_sfdcgitlabproduction') }}
    FROM {{ ref('user_sfdcgitlabproduction') }}

)

SELECT *
FROM source