{{
    simple_cte([
        ('survey_source', 'qualtrics_post_purchase_survey_responses_source')
    ]
    )
}} , parsed_json AS (

    SELECT
      IFF(response_values['finished']::NUMBER = 1, TRUE, FALSE)::BOOLEAN AS is_finished,
      response_values['plan']::VARCHAR                                   AS user_plan,
      response_values['account_id']::VARCHAR                             AS account_id,
      response_values['recordedDate']::TIMESTAMP                         AS recorded_at,
      response_values['startDate']::TIMESTAMP                            AS started_at,
      response_id
    FROM survey_source
)

SELECT *
FROM parsed_json
ORDER BY recorded_at DESC

