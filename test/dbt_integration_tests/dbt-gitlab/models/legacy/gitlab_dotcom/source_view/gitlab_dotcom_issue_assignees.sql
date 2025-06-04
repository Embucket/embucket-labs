

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issue_assignees_source') }}

)

SELECT *
FROM source
