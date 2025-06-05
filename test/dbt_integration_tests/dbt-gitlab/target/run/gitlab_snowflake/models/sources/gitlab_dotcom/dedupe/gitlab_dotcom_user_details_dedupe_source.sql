
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_user_details_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_user_details

QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY _uploaded_at DESC) = 1
    )
;


  