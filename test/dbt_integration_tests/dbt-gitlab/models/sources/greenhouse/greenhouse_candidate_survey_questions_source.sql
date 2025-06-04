WITH source as (

    SELECT *

      FROM {{ source('greenhouse', 'candidate_survey_questions') }}

), renamed as (

    SELECT
            --keys
            id::NUMBER            AS candidate_survey_question_id,

            --info
            question::varchar 		AS candidate_survey_question

    FROM source

)

SELECT *
FROM renamed
