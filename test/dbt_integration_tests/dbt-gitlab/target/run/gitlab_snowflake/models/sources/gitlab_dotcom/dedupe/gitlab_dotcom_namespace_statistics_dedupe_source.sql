
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_namespace_statistics_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_namespace_statistics

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
    )
;


  