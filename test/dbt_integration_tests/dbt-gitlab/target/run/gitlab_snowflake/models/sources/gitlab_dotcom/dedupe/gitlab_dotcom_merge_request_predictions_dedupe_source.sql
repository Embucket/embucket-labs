
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_merge_request_predictions_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_merge_request_predictions

QUALIFY ROW_NUMBER() OVER (PARTITION BY merge_request_id ORDER BY updated_at DESC) = 1
    )
;


  