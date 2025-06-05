
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_lfs_objects_dedupe_source
    

    
    as (


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_lfs_objects

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
    )
;


  