{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

  SELECT {{ nohash_sensitive_columns('nps_survey_response','user_email') }}
  FROM {{ref('nps_survey_response')}}

)

SELECT *
FROM source

