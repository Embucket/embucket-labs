
  
    

create or replace transient table EMBUCKET.legacy.gitlab_dotcom_epic_issues
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_epic_issues_source

)

SELECT *
FROM source
    )
;


  