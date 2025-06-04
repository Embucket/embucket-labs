WITH source as (

    SELECT *

      FROM {{ source('greenhouse', 'eeoc_responses') }}

), renamed as (

    SELECT

            --key
            application_id::NUMBER      AS application_id,

            --info
            status::varchar             AS candidate_status,
            race::varchar               AS candidate_race,
            gender::varchar             AS candidate_gender,
            disability_status::varchar  AS candidate_disability_status,
            veteran_status::varchar     AS candidate_veteran_status,
            submitted_at::timestamp     AS eeoc_response_submitted_at


    FROM source
    WHERE eeoc_response_submitted_at IS NOT NULL

)

SELECT *
FROM renamed
