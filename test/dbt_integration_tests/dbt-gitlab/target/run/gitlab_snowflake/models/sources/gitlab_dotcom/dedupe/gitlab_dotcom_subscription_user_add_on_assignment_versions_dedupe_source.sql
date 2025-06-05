
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_subscription_user_add_on_assignment_versions_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_subscription_user_add_on_assignment_versions

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
    )
;


  