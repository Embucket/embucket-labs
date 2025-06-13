WITH source as (

    SELECT *
    FROM {{ source('greenhouse', 'employments') }}

), renamed as (

    SELECT
                --key
                candidate_id::NUMBER        AS candidate_id,

                --info
                company_name::varchar       AS candidate_company_name,
                title::varchar              AS candidate_employment_title,
                "start"::date               AS candidate_employment_start_date,
                "end"::date                   AS candidate_employment_end_date,
                latest::boolean             AS is_candidate_latest_employment,
                created_at::timestamp       AS candidate_employement_created_at,
                updated_at::timestamp       AS candidate_employement_updated_at

    FROM source

)

SELECT *
FROM renamed
