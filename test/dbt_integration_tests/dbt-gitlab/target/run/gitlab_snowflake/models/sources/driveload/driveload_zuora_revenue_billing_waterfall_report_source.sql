
  
    

create or replace transient table EMBUCKET.driveload.driveload_zuora_revenue_billing_waterfall_report_source
    

    
    as (WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.zuora_revenue_billing_waterfall_report

)
SELECT * 
FROM source
    )
;


  