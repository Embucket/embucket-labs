WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_cluster_projects_dedupe_source') }}
      
)

, renamed AS (
  
    SELECT
      id::NUMBER           AS cluster_project_id,
      cluster_id::NUMBER   AS cluster_id,
      project_id::NUMBER   AS project_id
      
    FROM source
  
)

SELECT * 
FROM renamed
