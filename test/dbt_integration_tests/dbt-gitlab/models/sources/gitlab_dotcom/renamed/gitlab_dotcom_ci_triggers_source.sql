    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_triggers_dedupe_source') }}
  
), renamed AS (
  
  SELECT
  
    id::NUMBER           AS ci_trigger_id,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    project_id::NUMBER   AS project_id,
    owner_id::NUMBER     AS owner_id
    
  FROM source
  
)

SELECT * 
FROM renamed
