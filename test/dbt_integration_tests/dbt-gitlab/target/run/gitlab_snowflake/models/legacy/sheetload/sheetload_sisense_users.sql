
  
    

create or replace transient table EMBUCKET.legacy.sheetload_sisense_users
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_sisense_users_source

)

SELECT *
FROM source
    )
;


  