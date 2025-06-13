WITH source AS (

  SELECT *

  FROM {{ source('greenhouse', 'survey_builder_questions') }}

),

renamed AS (

  SELECT
    --keys
    id::NUMBER              AS question_id,
    organization_id::NUMBER AS organization_id,
    survey_id::NUMBER       AS survey_id,

    --info
    value::VARCHAR          AS candidate_survey_question,
    answer_type::VARCHAR    AS answer_type,
    priority::NUMBER        AS question_priority,
    active:BOOLEAN          AS is_question_active,
    created_at::TIMESTAMP   AS created_at,
    updated_at::TIMESTAMP   AS updated_at

  FROM source

)

SELECT *
FROM renamed
