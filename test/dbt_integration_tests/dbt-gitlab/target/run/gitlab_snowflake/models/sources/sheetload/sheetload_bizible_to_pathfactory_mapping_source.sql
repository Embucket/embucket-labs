
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_bizible_to_pathfactory_mapping_source
    

    
    as (WITH source AS (

        SELECT * 
        FROM EMBUCKET.sheetload.bizible_to_pathfactory_mapping

        )
        SELECT * 
        FROM source
    )
;


  