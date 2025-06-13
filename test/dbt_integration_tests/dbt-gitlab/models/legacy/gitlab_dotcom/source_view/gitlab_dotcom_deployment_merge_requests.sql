

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_deployment_merge_requests_source') }}

)

SELECT *
FROM source
