

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_resource_label_events_source') }}

)

SELECT *
FROM source
