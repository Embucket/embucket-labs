{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('customer_health_scorecard_fact_1') }}

)

SELECT *
FROM source