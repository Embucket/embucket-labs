
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_sdr_bdr_metric_targets_source
    

    
    as (WITH source AS (

    SELECT * 
    FROM EMBUCKET.sheetload.sdr_bdr_metric_targets

)

SELECT * 
FROM source
    )
;


  