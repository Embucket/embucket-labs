
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_users_ops_dashboard_projects_dedupe_source
    

    
    as (


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_users_ops_dashboard_projects

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
    )
;


  