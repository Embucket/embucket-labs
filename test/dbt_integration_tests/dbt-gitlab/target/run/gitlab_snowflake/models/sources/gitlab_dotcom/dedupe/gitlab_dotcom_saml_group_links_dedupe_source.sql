
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_saml_group_links_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_saml_group_links

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
    )
;


  