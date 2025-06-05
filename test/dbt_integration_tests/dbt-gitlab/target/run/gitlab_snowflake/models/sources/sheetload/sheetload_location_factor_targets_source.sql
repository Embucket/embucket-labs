
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_location_factor_targets_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.location_factor_targets

), renamed AS (

    SELECT 
      department::VARCHAR AS department,
      target::FLOAT       AS location_factor_target
    FROM source

)

SELECT *
FROM renamed
    )
;


  