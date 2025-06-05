
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_cert_pricing_customer_discount_dashboard_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.cert_pricing_customer_discount_dashboard

)

SELECT *
FROM source
    )
;


  