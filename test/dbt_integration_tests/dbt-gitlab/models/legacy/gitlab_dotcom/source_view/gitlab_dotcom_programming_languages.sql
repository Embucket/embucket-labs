

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_programming_languages_source') }}

)

SELECT *
FROM source
