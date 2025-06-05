
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_product_maturity_historical_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.product_maturity_historical

)

SELECT * 
FROM source
    )
;


  