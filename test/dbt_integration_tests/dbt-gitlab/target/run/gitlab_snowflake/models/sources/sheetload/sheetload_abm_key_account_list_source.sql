
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_abm_key_account_list_source
    

    
    as (WITH source AS (

        SELECT * 
        FROM EMBUCKET.sheetload.abm_key_account_list

        )
        SELECT * 
        FROM source
    )
;


  