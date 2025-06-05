
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_web_hooks_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_web_hooks

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
    )
;


  