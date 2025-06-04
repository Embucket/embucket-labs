WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_approval_project_rules_users_dedupe_source') }}
  
), renamed AS (

  SELECT
    id::NUMBER                          AS project_rules_users_id,
    approval_project_rule_id::NUMBER    AS approval_project_rule_id,
    user_id::NUMBER                     AS user_id

  FROM source

)

SELECT *
FROM renamed