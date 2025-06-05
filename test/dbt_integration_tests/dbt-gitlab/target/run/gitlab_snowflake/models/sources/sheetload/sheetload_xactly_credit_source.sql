
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_xactly_credit_source
    

    
    as (WITH source AS (

  SELECT * 
  FROM EMBUCKET.sheetload.xactly_credit_sheetload

)
SELECT * 
FROM source
    )
;


  