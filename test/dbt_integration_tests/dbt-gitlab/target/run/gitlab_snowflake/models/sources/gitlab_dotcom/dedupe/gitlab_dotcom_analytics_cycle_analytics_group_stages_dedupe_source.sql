
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_analytics_cycle_analytics_group_stages_dedupe_source
    

    
    as (


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_analytics_cycle_analytics_group_stages

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
    )
;


  