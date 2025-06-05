
  
    

create or replace transient table EMBUCKET.legacy.sheetload_product_maturity_historical
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_product_maturity_historical_source

)

SELECT *
FROM source
    )
;


  