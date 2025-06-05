
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_status_page_published_incidents_dedupe_source
    

    
    as (


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_status_page_published_incidents

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
    )
;


  