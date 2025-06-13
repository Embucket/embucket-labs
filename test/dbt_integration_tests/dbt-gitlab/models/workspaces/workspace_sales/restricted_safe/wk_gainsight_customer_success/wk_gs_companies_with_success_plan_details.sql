{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT
    {{ hash_sensitive_columns('companies_with_success_plan_details') }}
    FROM {{ ref('companies_with_success_plan_details') }}

)

SELECT *
FROM source