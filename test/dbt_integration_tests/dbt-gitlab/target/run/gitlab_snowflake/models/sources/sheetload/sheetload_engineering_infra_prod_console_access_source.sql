
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_engineering_infra_prod_console_access_source
    

    
    as (WITH source AS (
    
    SELECT * 
    FROM EMBUCKET.sheetload.engineering_infra_prod_console_access
)

SELECT *
FROM source
    )
;


  