
  
    

create or replace transient table EMBUCKET.legacy.sheetload_gitlab_contributing_organizations
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_gitlab_contributing_organizations_source

)

SELECT *
FROM source
    )
;


  