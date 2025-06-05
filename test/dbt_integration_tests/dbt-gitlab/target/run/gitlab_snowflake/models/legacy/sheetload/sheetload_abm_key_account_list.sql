
  
    

create or replace transient table EMBUCKET.legacy.sheetload_abm_key_account_list
    

    
    as (WITH source AS (

        SELECT * 
        FROM EMBUCKET.sheetload.sheetload_abm_key_account_list_source

        )
        SELECT * 
        FROM source
    )
;


  