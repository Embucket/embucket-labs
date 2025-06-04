WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_secure_files_source') }}

)

SELECT *
FROM source
