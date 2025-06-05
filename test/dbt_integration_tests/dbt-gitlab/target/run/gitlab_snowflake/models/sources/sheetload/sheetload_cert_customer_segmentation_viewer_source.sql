
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_cert_customer_segmentation_viewer_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.cert_customer_segmentation_viewer

)

SELECT *
FROM source
    )
;


  