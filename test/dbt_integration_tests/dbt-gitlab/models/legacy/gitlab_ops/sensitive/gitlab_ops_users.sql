WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_ops_users_source') }}

)

SELECT *
FROM source
