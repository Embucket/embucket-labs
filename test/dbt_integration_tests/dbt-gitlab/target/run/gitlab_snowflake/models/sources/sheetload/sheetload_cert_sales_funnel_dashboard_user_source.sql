
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_cert_sales_funnel_dashboard_user_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.cert_sales_funnel_dashboard_user

)

SELECT *
FROM source
    )
;


  