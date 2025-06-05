
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_alert_management_alert_assignees_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_alert_management_alert_assignees

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
    )
;


  