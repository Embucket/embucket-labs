
  
    

create or replace transient table EMBUCKET.legacy.sheetload_fmm_kpi_targets
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_fmm_kpi_targets_source

)

SELECT *
FROM source
    )
;


  