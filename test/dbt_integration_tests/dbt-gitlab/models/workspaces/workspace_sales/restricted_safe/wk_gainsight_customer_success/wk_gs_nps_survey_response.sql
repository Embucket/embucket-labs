{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT
    {{ hash_sensitive_columns('nps_survey_response') }}
    FROM {{ ref('nps_survey_response') }}

)

SELECT *
FROM source