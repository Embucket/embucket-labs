

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_milestones_source') }}

)

SELECT *
FROM source
