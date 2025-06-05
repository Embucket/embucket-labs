
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_epics_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_epics

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
    )
;


  