

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_repository_languages_source') }}

)

SELECT *
FROM source
