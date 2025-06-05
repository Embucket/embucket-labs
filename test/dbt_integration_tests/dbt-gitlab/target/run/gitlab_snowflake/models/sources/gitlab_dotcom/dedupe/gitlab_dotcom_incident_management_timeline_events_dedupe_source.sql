
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_incident_management_timeline_events_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_incident_management_timeline_events

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
    )
;


  