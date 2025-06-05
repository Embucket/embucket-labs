
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_protected_branch_merge_access_levels_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_protected_branch_merge_access_levels

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
    )
;


  