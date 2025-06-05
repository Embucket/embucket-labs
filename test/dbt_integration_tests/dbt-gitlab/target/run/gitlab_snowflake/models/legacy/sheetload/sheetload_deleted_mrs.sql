
  
    

create or replace transient table EMBUCKET.legacy.sheetload_deleted_mrs
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_deleted_mrs_source

)

SELECT *
FROM source
    )
;


  