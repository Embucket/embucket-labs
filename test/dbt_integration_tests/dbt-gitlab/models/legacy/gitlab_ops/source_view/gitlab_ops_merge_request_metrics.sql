WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_ops_merge_request_metrics_source') }}

)

SELECT *
FROM source
