
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_user_preferences_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_user_preferences

QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) = 1
    )
;


  