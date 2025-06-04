

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_projects_source') }}

)

SELECT *
FROM source
