WITH source as (

    SELECT *
    FROM {{ source('greenhouse', 'candidates') }}

), renamed as (

    SELECT
            --keys
            id::NUMBER              AS candidate_id,

            --info
            company::varchar        AS candidate_company,
            title::varchar          AS candidate_title,
            created_at::timestamp   AS candidate_created_at,
            updated_at::timestamp   AS candidate_updated_at,
            migrated::boolean       AS is_candidate_migrated,
            private::boolean        AS is_candidate_private

    FROM source

)

SELECT *
FROM renamed
