
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_cert_product_geo_dashboard_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.cert_product_geo_dashboard

)

SELECT *
FROM source
    )
;


  