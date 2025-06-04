WITH answers AS (

    SELECT *
    FROM {{ ref('qualtrics_nps_answers') }}
    WHERE question_id IN ('QID172787673', 'QID172787675_TEXT')

), trimmed AS (

    SELECT
      response_id,
      distribution_channel,
      has_finished_survey,
      user_language,
      survey_start_date,
      survey_end_date,
      response_recorded_at,
      user_plan,
      MAX(IFF(question_id = 'QID172787673', question_response, NULL)::NUMBER)       AS nps_score,
      MAX(IFF(question_id = 'QID172787675_TEXT', question_response, NULL)::VARCHAR) AS nps_reason
    FROM answers
    {{ dbt_utils.group_by(n=8) }}

), final AS (

    SELECT *,
      CASE
        WHEN nps_score >= 9 THEN 'promoter'
        WHEN nps_score >= 7 THEN 'passive'
        WHEN nps_score >= 0 THEN 'detractor'
      END AS nps_bucket_text,
      CASE
        WHEN nps_bucket_text = 'promoter'  THEN 100
        WHEN nps_bucket_text = 'passive'   THEN 0
        WHEN nps_bucket_text = 'detractor' THEN -100
      END AS nps_bucket_integer
  FROM trimmed

)

SELECT *
FROM final
ORDER BY response_recorded_at
