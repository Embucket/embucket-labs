WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_epic_issues_dedupe_source') }}
  
), renamed AS (

    SELECT
      id::NUMBER                       AS epic_issues_relation_id,
      epic_id::NUMBER                  AS epic_id,
      issue_id::NUMBER                 AS issue_id,
      relative_position::NUMBER        AS epic_issue_relative_position

    FROM source

)


SELECT *
FROM renamed
