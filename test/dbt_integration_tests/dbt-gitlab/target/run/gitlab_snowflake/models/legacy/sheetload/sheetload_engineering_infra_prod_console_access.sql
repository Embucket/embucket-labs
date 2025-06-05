
  
    

create or replace transient table EMBUCKET.legacy.sheetload_engineering_infra_prod_console_access
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_engineering_infra_prod_console_access_source

)

SELECT *
FROM source
    )
;


  