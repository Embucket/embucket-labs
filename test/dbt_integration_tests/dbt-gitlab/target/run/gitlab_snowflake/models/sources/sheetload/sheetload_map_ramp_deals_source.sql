
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_map_ramp_deals_source
    

    
    as (WITH source AS (

  SELECT * 
  FROM EMBUCKET.sheetload.map_ramp_deals

)
SELECT * 
FROM source
    )
;


  