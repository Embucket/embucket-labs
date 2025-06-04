WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_cluster_groups_dedupe_source') }}
      
)

, renamed AS (
  
    SELECT
    
      id::NUMBER           AS cluster_group_id,
      cluster_id::NUMBER   AS cluster_id,
      group_id::NUMBER     AS group_id

    FROM source
  
)

SELECT * 
FROM renamed
