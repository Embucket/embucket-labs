{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('ctas_healthscores') }}

)

SELECT *
FROM source