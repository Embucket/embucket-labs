WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_ops_merge_requests_source') }}

)

SELECT *
FROM source
