
  
    

create or replace transient table EMBUCKET.workspace_product.gitlab_dotcom_fork_network_members
    

    
    as (WITH source AS (
	SELECT *
	FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_fork_network_members_source
)
SELECT *
FROM source
    )
;


  