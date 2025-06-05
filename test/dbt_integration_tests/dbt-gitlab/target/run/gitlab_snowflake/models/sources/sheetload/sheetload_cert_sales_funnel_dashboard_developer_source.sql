
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_cert_sales_funnel_dashboard_developer_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.cert_sales_funnel_dashboard_developer

)

SELECT *
FROM source
    )
;


  