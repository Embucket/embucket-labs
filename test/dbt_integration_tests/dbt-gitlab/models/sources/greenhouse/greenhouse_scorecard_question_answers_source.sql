WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'scorecard_question_answers') }}

), renamed as (

	SELECT

            --keys
            scorecard_id::NUMBER                AS scorecard_id,
            application_id::NUMBER              AS application_id,

            --info
            question::varchar                   AS scorecard_question,
            answer::varchar                     AS scorecard_answer,
            created_at::timestamp               AS scorecard_question_answer_created_at,
            updated_at::varchar::timestamp      AS scorecard_question_answer_updated_at

	FROM source

)

SELECT *
FROM renamed
