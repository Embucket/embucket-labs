WITH source AS (

  SELECT *

  FROM {{ source('greenhouse', 'survey_builder_answers') }}

),

renamed AS (

  SELECT
    --keys
    id::NUMBER                            AS answer_id,
    question_id::NUMBER                   AS question_id,
    organization_id::NUMBER               AS organization_id,
    survey_id::NUMBER                     AS survey_id,
    delivered_candidate_survey_id::NUMBER AS delivered_candidate_survey_id,

    --info
    value::VARCHAR                        AS answer,
    comment::VARCHAR                      AS comment,
    created_at::TIMESTAMP                 AS created_at,
    updated_at::TIMESTAMP                 AS updated_at,
    submitted_at::TIMESTAMP               AS submitted_at

  FROM source

)

SELECT *
FROM renamed
