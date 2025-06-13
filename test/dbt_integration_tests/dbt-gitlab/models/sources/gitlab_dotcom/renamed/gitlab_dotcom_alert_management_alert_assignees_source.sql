WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_alert_management_alert_assignees_dedupe_source') }}
    
), renamed AS (

    SELECT
      id::NUMBER                AS alert_management_alert_assignee_id,
      user_id::NUMBER           AS user_id,
      alert_id::NUMBER          AS alert_id

    FROM source

)

SELECT *
FROM renamed
