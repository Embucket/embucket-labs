
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_ci_sources_pipelines_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_ci_sources_pipelines

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
    )
;


  