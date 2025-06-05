WITH source AS (

  SELECT DISTINCT
    user_id,
    issue_id
  FROM EMBUCKET.tap_postgres.gitlab_db_issue_assignees

), renamed AS (

    SELECT
      md5(user_id || issue_id)::VARCHAR AS user_issue_relation_id,
      user_id::NUMBER                  AS user_id,
      issue_id::NUMBER                 AS issue_id
    FROM source


)

SELECT DISTINCT *
FROM renamed