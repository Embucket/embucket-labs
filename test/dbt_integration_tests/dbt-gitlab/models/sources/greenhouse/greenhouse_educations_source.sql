WITH source as (

    SELECT *

      FROM {{ source('greenhouse', 'educations') }}

), renamed as (

    SELECT

                --key
                candidate_id::NUMBER    AS candidate_id,

                --info
                school_name::varchar    AS candidate_school_name,
                degree::varchar         AS candidate_degree,
                discipline::varchar     AS candidate_discipline,
                "start"::date           AS candidate_education_start_date,
                "end"::date               AS candidate_education_end_date,
                latest::boolean         AS candidate_latest_education,
                created_at::timestamp   AS candidate_education_created_at,
                updated_at::timestamp   AS candidate_education_updated_at


    FROM source

)

SELECT *
FROM renamed
