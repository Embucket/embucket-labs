
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_ga360_custom_dimensions_source
    

    
    as (WITH source AS (

        SELECT 
            dimension_name::VARCHAR AS dimension_name,
            dimension_index::NUMBER AS dimension_index,
            dimension_scope::VARCHAR AS dimension_scope
        FROM EMBUCKET.sheetload.ga360_customdimensions

)

SELECT * 
FROM source
    )
;


  