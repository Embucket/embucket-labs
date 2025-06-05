
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_epic_issues_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_epic_issues

QUALIFY ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY _uploaded_at DESC) = 1
    )
;


  