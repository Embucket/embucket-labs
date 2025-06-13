

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_deployments_source') }}

)

SELECT *
FROM source
