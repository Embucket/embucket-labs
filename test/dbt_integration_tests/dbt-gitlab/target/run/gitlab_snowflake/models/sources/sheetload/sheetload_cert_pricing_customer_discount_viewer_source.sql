
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_cert_pricing_customer_discount_viewer_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.cert_pricing_customer_discount_viewer

)

SELECT *
FROM source
    )
;


  