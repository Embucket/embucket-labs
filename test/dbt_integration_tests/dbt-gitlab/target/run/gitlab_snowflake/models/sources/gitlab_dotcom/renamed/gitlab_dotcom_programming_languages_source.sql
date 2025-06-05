
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_programming_languages_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_programming_languages_dedupe_source
    
), renamed AS (

    SELECT
      id::NUMBER                       AS programming_language_id,
      name::VARCHAR                     AS programming_language_name
    FROM source

)

SELECT *
FROM renamed
    )
;


  