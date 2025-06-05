
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_ci_builds_metadata_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_ci_builds_metadata

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _UPLOADED_AT DESC) = 1
    )
;


  