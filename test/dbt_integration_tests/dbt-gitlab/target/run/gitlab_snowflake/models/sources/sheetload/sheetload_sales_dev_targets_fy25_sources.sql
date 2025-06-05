
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_sales_dev_targets_fy25_sources
    

    
    as (WITH source AS (

        SELECT * 
        FROM EMBUCKET.sheetload.sales_dev_targets_fy25

        )
        SELECT * 
        FROM source
    )
;


  