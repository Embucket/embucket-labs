
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_cert_customer_segmentation_dashboard_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.cert_customer_segmentation_dashboard

)

SELECT *
FROM source
    )
;


  