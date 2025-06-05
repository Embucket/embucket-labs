
  
    

create or replace transient table EMBUCKET.legacy.gitlab_dotcom_saml_providers
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_saml_providers_source

)

SELECT *
FROM source
    )
;


  