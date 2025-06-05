
  
    

create or replace transient table EMBUCKET.legacy.sheetload_bizible_to_pathfactory_mapping
    

    
    as (WITH source AS (

        SELECT * 
        FROM EMBUCKET.sheetload.sheetload_bizible_to_pathfactory_mapping_source

        )
        SELECT * 
        FROM source
    )
;


  