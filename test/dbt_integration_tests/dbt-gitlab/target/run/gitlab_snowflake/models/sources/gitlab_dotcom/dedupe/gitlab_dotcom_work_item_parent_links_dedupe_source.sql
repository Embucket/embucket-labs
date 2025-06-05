
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_work_item_parent_links_dedupe_source
    

    
    as (


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_work_item_parent_links

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
    )
;


  