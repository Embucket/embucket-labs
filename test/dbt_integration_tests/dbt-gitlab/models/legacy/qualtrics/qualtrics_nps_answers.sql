{{ config({
    "schema": "sensitive",
    "database": env_var('SNOWFLAKE_PREP_DATABASE'),
    })
}}
WITH responses AS (

    SELECT *
    FROM {{ ref('qualtrics_nps_survey_responses') }}

), questions AS (

    SELECT 
      *,
      IFNULL(answer_choices[0]['1']['TextEntry'] = 'on', IFNULL(ARRAY_SIZE(answer_choices) = 0, TRUE)) AS is_free_text
    FROM {{ ref('qualtrics_question') }}

), revised_question_ids AS (
    
    SELECT
      question_description,
      IFF(is_free_text, question_id || '_TEXT', question_id) AS question_id
    FROM questions

), parsed_out_qas AS (

    SELECT 
      response_id,
      question_id,
      question_description,
      GET(response_values, question_id)                 AS question_response,
      response_values['distributionChannel']::VARCHAR   AS distribution_channel,
      IFF(response_values['finished'] = 1, True, False) AS has_finished_survey,
      response_values['startDate']::TIMESTAMP           AS survey_start_date,
      response_values['endDate']::TIMESTAMP             AS survey_end_date,
      response_values['recordedDate']::TIMESTAMP        AS response_recorded_at,
      response_values['userLanguage']::VARCHAR          AS user_language,
      GET(response_values, 'plan')::VARCHAR             AS user_plan
    FROM revised_question_ids 
    INNER JOIN responses
    ON GET(response_values, question_id) IS NOT NULL
)

SELECT *
FROM parsed_out_qas
