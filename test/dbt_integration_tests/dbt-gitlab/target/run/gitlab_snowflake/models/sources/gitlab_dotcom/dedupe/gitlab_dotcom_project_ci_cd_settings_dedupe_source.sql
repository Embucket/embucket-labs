
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_project_ci_cd_settings_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_project_ci_cd_settings

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
    )
;


  