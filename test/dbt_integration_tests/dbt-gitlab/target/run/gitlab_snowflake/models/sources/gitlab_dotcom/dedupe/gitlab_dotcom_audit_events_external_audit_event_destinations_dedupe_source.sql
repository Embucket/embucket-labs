
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_audit_events_external_audit_event_destinations_dedupe_source
    

    
    as (


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_audit_events_external_audit_event_destinations

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
    )
;


  