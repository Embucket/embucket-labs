

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_resource_weight_events_source') }}

)

SELECT *
FROM source
