
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_finance.driveload_pending_invoices_report
    

    
    as (WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.driveload_pending_invoices_report_source

)
SELECT * 
FROM source
    )
;


  