WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_trigger_requests_dedupe_source') }}
  
), renamed AS (
  
  SELECT

    id::NUMBER           AS ci_trigger_request_id,
    trigger_id::NUMBER   AS trigger_id,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    commit_id::NUMBER    AS commit_id
    
  FROM source
  
)

SELECT * 
FROM renamed
