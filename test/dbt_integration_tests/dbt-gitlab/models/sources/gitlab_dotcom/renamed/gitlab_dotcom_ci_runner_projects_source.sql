WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_runner_projects_dedupe_source') }}
      
)

, renamed AS (
  
    SELECT
    
      id::NUMBER           AS ci_runner_project_id,
      runner_id::NUMBER    AS runner_id,
      project_id::NUMBER   AS project_id,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at

    FROM source
    WHERE project_id IS NOT NULL
  
)

SELECT * 
FROM renamed
