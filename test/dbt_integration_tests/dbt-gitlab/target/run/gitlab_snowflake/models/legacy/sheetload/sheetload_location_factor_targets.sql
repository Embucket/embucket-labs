
  
    

create or replace transient table EMBUCKET.legacy.sheetload_location_factor_targets
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_location_factor_targets_source

)

SELECT *
FROM source
    )
;


  