
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_workspaces_agent_configs_dedupe_source
    

    
    as (






SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_workspaces_agent_configs

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
    )
;


  