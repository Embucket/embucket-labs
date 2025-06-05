
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_percent_over_comp_band_historical_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.percent_over_comp_band_historical

)

SELECT *
FROM source
    )
;


  