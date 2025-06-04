WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_subscriptions_projects_dedupe_source') }}

), renamed AS (
  
  SELECT
    id::NUMBER                        AS ci_subscriptions_projects_id,
    downstream_project_id::NUMBER     AS downstream_project_id,
    upstream_project_id::NUMBER       AS upstream_project_id,
    author_id::NUMBER                 AS author_id
  FROM source

)

SELECT *
FROM renamed
