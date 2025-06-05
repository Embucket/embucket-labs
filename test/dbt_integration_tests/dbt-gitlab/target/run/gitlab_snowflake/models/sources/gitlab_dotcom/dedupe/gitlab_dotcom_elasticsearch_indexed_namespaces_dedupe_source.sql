
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_elasticsearch_indexed_namespaces_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_elasticsearch_indexed_namespaces

QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY updated_at DESC) = 1
    )
;


  