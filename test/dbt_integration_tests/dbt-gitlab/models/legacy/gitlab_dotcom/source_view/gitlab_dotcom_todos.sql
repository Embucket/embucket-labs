

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_todos_source') }}

)

SELECT *
FROM source