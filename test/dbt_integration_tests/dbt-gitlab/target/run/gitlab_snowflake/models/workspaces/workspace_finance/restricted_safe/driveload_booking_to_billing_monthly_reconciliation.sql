
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_finance.driveload_booking_to_billing_monthly_reconciliation
    

    
    as (WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.driveload_booking_to_billing_monthly_reconciliation_source

)
SELECT * 
FROM source
    )
;


  