WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_approval_project_rules_groups_dedupe_source') }}
  
), renamed AS (

  SELECT
    id::NUMBER                          AS project_rules_groups_id,
    approval_project_rule_id::NUMBER    AS approval_project_rule_id,
    group_id::NUMBER                     AS group_id

  FROM source

)

SELECT *
FROM renamed