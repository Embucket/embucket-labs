
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_finance.driveload_invoice_aging_detail
    

    
    as (WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.driveload_invoice_aging_detail_source

)
SELECT * 
FROM source
    )
;


  