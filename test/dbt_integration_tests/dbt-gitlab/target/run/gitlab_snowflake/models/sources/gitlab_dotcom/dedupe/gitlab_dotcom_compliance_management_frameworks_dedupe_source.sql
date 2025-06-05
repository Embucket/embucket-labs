
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_compliance_management_frameworks_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_compliance_management_frameworks

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
    )
;


  