WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_job_roles_prior_to_2020_02_source') }}

)

SELECT *
FROM source