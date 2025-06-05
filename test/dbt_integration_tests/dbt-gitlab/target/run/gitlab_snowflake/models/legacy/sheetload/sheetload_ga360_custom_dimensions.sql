
  
    

create or replace transient table EMBUCKET.legacy.sheetload_ga360_custom_dimensions
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_ga360_custom_dimensions_source

)

SELECT *
FROM source
    )
;


  