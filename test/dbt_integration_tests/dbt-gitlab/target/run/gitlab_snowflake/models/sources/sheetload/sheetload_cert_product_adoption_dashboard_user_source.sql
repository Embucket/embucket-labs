
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_cert_product_adoption_dashboard_user_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.cert_product_adoption_dashboard_user

)

SELECT *
FROM source
    )
;


  