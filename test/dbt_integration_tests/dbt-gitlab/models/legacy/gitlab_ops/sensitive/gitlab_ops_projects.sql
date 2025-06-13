WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_ops_projects_source') }}

)

SELECT *
FROM source
