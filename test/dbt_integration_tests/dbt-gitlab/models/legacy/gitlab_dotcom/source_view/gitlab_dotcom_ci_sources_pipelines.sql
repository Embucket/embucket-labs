

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_sources_pipelines_source') }}

)

SELECT *
FROM source
