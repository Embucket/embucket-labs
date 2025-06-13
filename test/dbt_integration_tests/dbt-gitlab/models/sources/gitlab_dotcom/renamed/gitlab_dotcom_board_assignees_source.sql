WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_board_assignees_dedupe_source') }}
  
), renamed AS (

  SELECT
    id::NUMBER                 AS board_assignee_relation_id,
    board_id::NUMBER           AS board_id,
    assignee_id::NUMBER        AS board_assignee_id

  FROM source

)

SELECT *
FROM renamed
