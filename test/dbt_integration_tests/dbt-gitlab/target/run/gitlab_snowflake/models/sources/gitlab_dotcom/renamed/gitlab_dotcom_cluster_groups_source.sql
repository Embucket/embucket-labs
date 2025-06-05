
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_cluster_groups_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_cluster_groups_dedupe_source
      
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
    )
;


  