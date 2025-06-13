WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_linkedin_recruiter_source') }}

)

SELECT *
FROM source