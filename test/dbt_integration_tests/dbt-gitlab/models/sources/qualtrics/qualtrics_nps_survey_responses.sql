{{ config({
    "schema": "sensitive",
    "database": env_var('SNOWFLAKE_PREP_DATABASE'),
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('qualtrics', 'nps_survey_responses') }}

), parsed AS (

    SELECT 
      d.value as data_by_row,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext['responses']), outer => true) d

), response_parsed AS (

    SELECT 
      data_by_row['responseId']::VARCHAR  AS response_id,
      data_by_row['values']::VARIANT      AS response_values
    FROM parsed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY response_id ORDER BY uploaded_at DESC) = 1

)
SELECT * 
FROM response_parsed
