

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_environments_source') }}

)

SELECT *
FROM source
