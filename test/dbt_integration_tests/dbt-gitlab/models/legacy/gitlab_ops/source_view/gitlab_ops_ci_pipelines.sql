WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_ops_ci_pipelines_source') }}

)

SELECT *
FROM source
