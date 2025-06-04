WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_ops_ci_stages_source') }}

)

SELECT *
FROM source

