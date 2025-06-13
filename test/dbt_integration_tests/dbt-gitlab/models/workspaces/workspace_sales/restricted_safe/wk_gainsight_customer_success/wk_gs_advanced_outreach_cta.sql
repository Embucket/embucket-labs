{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('advanced_outreach_cta') }}

)

SELECT *
FROM source