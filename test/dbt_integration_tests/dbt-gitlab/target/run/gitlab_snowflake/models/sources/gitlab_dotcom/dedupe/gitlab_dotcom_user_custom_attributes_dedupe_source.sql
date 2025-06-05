
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_user_custom_attributes_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_user_custom_attributes

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
    )
;


  