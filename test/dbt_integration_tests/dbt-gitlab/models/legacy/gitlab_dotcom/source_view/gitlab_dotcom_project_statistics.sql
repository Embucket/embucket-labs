

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_project_statistics_source') }}

)

SELECT *
FROM source
