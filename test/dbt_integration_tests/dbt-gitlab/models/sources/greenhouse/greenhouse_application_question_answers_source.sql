WITH source as (

    SELECT *
        FROM {{ source('greenhouse', 'application_question_answers') }}

), renamed as (

    SELECT

            --keys
            job_post_id::NUMBER          AS job_post_id,
            application_id::NUMBER       AS application_id,

            --info
            question::varchar            AS application_question,
            answer::varchar              AS application_answer,

            created_at::timestamp        AS application_question_answer_created_at,
            updated_at::timestamp        AS application_question_answer_updated_at

    FROM source

)

SELECT *
FROM renamed
