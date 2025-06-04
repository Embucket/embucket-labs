{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('companies_with_success_plan_objectives') }}

)

SELECT *
FROM source