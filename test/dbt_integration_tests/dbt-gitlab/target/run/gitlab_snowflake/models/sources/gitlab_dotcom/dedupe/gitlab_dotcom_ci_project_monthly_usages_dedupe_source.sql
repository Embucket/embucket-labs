
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_ci_project_monthly_usages_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_ci_project_monthly_usages

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
    )
;


  