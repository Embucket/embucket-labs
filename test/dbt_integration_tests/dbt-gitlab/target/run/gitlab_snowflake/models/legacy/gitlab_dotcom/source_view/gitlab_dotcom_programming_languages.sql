
  
    

create or replace transient table EMBUCKET.legacy.gitlab_dotcom_programming_languages
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_programming_languages_source

)

SELECT *
FROM source
    )
;


  