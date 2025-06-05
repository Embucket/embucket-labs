
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_approval_merge_request_rules_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_approval_merge_request_rules

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
    )
;


  