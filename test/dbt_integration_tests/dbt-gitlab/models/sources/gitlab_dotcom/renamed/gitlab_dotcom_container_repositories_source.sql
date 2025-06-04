WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_container_repositories_dedupe_source') }}

),

renamed AS (

  SELECT
    id::NUMBER AS container_repository_id,
    project_id::NUMBER AS project_id,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at
  FROM source 

)

SELECT * 
FROM renamed
