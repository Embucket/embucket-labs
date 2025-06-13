WITH responses AS (

    SELECT *
    FROM {{ ref('qualtrics_post_purchase_survey_responses_source') }}

), questions AS (

    SELECT
      *,
      IFNULL(answer_choices[0]['1']['TextEntry'] = 'on', IFNULL(ARRAY_SIZE(answer_choices) = 0, TRUE)) AS is_free_text
    FROM {{ ref('qualtrics_question') }}

), revised_question_ids AS (

    SELECT
      question_description,
      answer_choices,
      IFF(is_free_text, question_id || '_TEXT', question_id) AS question_id
    FROM questions

), parsed_out_qas AS (

    SELECT
      response_id,
      question_id,
      question_description,
      GET(response_values, question_id)                 AS question_response,
      answer_choices[0]                                 AS answer_choices,
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
    WHERE QUESTION_ID != 'QID4_TEXT' AND ARRAY_SIZE(QUESTION_RESPONSE) > 0

), final AS (

    SELECT
      GET(answer_choices, d.value)['Display']::VARCHAR AS answer_display,
      d.value::VARCHAR || question_id                  AS answer_id,
      distribution_channel,
      has_finished_survey,
      response_id,
      question_description,
      question_id,
      response_recorded_at,
      survey_end_date,
      survey_start_date,
      user_language,
      user_plan
    FROM parsed_out_qas,
    LATERAL FLATTEN(input => question_response) d
    WHERE answer_display IS NOT NULL

), manually_uploaded_data AS (
    -- 118 rows
    select
        answer_display,
        answer_id,
        distribution_channel,
        has_finished_survey,
        response_id,
        question_description,
        question_id,
        TO_TIMESTAMP_NTZ(response_recorded_at) as response_recorded_at,
        TO_TIMESTAMP_NTZ(survey_end_date) as survey_end_date,
        TO_TIMESTAMP_NTZ(survey_start_date) as survey_start_date,
        user_language,
        user_plan
    from {{ ref('qualtrics_post_purchase_survey_answers_from_issue_15225') }} 
)

SELECT *
FROM final
UNION
SELECT *
FROM manually_uploaded_data