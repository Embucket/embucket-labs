{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('comments') }}

)

SELECT *
FROM source