{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('advanced_outreach_participant_activity') }}

)

SELECT *
FROM source