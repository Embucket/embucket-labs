
  
    

create or replace transient table EMBUCKET.legacy.gitlab_dotcom_deployment_merge_requests
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_deployment_merge_requests_source

)

SELECT *
FROM source
    )
;


  