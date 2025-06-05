
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_deployment_merge_requests_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_deployment_merge_requests

QUALIFY ROW_NUMBER() OVER (PARTITION BY deployment_merge_request_id ORDER BY _uploaded_at DESC) = 1
    )
;


  