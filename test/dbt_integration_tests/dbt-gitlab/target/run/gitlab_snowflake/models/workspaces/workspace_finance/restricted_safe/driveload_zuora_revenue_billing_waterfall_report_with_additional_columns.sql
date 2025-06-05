
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_finance.driveload_zuora_revenue_billing_waterfall_report_with_additional_columns
    

    
    as (

WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.driveload_zuora_revenue_billing_waterfall_report_with_additional_columns_source

)
SELECT * 
FROM source
    )
;


  