
  
    

create or replace transient table EMBUCKET.legacy.sheetload_percent_over_comp_band_historical
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_percent_over_comp_band_historical_source

)

SELECT *
FROM source
    )
;


  