
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_namespace_settings_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_namespace_settings

QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY updated_at DESC) = 1
    )
;


  