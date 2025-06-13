{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('account_scorecard_history') }}

)

SELECT *
FROM source